package cn.com.multi_writer.sink

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadPoolExecutor, TimeUnit}

/**
 * 写入任务封装
 *
 * @param batchDF       需要写入的DataFrame
 * @param hudiTableName Hudi表名
 * @param hudiTablePath Hudi表存储路径
 * @param taskId        任务ID
 * @param options       额外的写入选项
 */
case class WriteTask(
                        batchDF: DataFrame,
                        hudiTableName: String,
                        hudiTablePath: String,
                        taskId: String,
                        options: Map[String, String] = Map.empty
                    ) {
    require(batchDF != null, "DataFrame不能为null")
    require(hudiTableName != null && hudiTableName.nonEmpty, "表名不能为空")
    require(hudiTablePath != null && hudiTablePath.nonEmpty, "表路径不能为空")
    require(taskId != null && taskId.nonEmpty, "任务ID不能为空")
}

/**
 * 写入结果封装
 *
 * @param taskId        任务ID
 * @param hudiTableName Hudi表名
 * @param isSuccess     是否成功
 * @param errorMsg      错误信息（如果失败）
 * @param exception     异常信息（如果失败）
 * @param executionTime 执行时间（毫秒）
 */
case class WriteResult(
                          taskId: String,
                          hudiTableName: String,
                          isSuccess: Boolean,
                          errorMsg: Option[String] = None,
                          exception: Option[Throwable] = None,
                          executionTime: Long = 0L
                      ) {
    def isFailure: Boolean = !isSuccess
}

/**
 * 写入任务队列管理器
 * 用于管理待处理的写入任务
 */
class WriteTaskQueue {
    private val queue = new ConcurrentLinkedQueue[WriteTask]()
    private val logger: Logger = LoggerFactory.getLogger(classOf[WriteTaskQueue])

    /**
     * 添加单个写入任务
     *
     * @param task 写入任务
     */
    def addTask(task: WriteTask): Unit = {
        queue.offer(task)
        logger.info(s"添加写入任务到队列: ${task.taskId} - ${task.hudiTableName}")
    }

    /**
     * 批量添加写入任务
     *
     * @param tasks 写入任务列表
     */
    def addTasks(tasks: Seq[WriteTask]): Unit = {
        tasks.foreach(addTask)
        logger.info(s"批量添加 ${tasks.length} 个写入任务到队列")
    }

    /**
     * 取出一个任务（非阻塞）
     *
     * @return 任务Option
     */
    def pollTask(): Option[WriteTask] = {
        Option(queue.poll())
    }

    /**
     * 取出所有任务
     *
     * @return 任务列表
     */
    def pollAllTasks(): Seq[WriteTask] = {
        val tasks = mutable.ListBuffer[WriteTask]()
        var task = queue.poll()
        while (task != null) {
            tasks += task
            task = queue.poll()
        }
        tasks.toSeq
    }

    /**
     * 获取队列大小
     *
     * @return 队列大小
     */
    def size(): Int = queue.size()

    /**
     * 队列是否为空
     *
     * @return 是否为空
     */
    def isEmpty: Boolean = queue.isEmpty
}

/**
 * 写入任务执行器
 * 负责执行单个写入任务
 */
class WriteTaskExecutor(spark: SparkSession) {
    private val logger: Logger = LoggerFactory.getLogger(classOf[WriteTaskExecutor])

    /**
     * 执行写入任务
     *
     * @param task 写入任务
     * @return 写入结果
     */
    def execute(task: WriteTask): WriteResult = {
        val startTime = System.currentTimeMillis()
        logger.info(s"开始执行写入任务: ${task.taskId} - ${task.hudiTableName}")

        val result = Try {
            // 构建写入配置
            val writer = task.batchDF
                .write
                .format("hudi")
                .option("hoodie.table.name", task.hudiTableName)
                .mode("append")

            // 应用额外的配置选项
            val finalWriter = task.options.foldLeft(writer) { case (w, (key, value)) =>
                w.option(key, value)
            }

            // 执行写入
            finalWriter.save(task.hudiTablePath)

            val executionTime = System.currentTimeMillis() - startTime
            logger.info(s"写入任务执行成功: ${task.taskId} - ${task.hudiTableName}, 耗时: ${executionTime}ms")

            WriteResult(
                taskId = task.taskId,
                hudiTableName = task.hudiTableName,
                isSuccess = true,
                executionTime = executionTime
            )
        } match {
            case Success(result) => result
            case Failure(exception) =>
                val executionTime = System.currentTimeMillis() - startTime
                val errorMsg = s"写入任务执行失败: ${task.taskId} - ${task.hudiTableName}, 错误: ${exception.getMessage}"
                logger.error(errorMsg, exception)

                WriteResult(
                    taskId = task.taskId,
                    hudiTableName = task.hudiTableName,
                    isSuccess = false,
                    errorMsg = Some(errorMsg),
                    exception = Some(exception),
                    executionTime = executionTime
                )
        }

        result
    }
}

/**
 * Hudi并发写入器
 * 支持并发写入多个Hudi表
 */
class HudiConcurrentWriter(
                              spark: SparkSession,
                              maxConcurrency: Int = 5,
                              timeoutSeconds: Long = 300,
                              failFast: Boolean = true
                          ) {

    require(maxConcurrency > 0, "最大并发数必须大于0")
    require(timeoutSeconds > 0, "超时时间必须大于0")

    private val logger: Logger = LoggerFactory.getLogger(classOf[HudiConcurrentWriter])
    private val taskQueue = new WriteTaskQueue()
    private val executor = new WriteTaskExecutor(spark)

    // 创建线程池
    private val threadPool = Executors.newFixedThreadPool(maxConcurrency).asInstanceOf[ThreadPoolExecutor]
    private implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

    /**
     * 提交单个写入任务
     *
     * @param batchDF       需要写入的DataFrame
     * @param hudiTableName Hudi表名
     * @param hudiTablePath Hudi表存储路径
     * @param taskId        任务ID（可选，会自动生成）
     * @param options       额外的写入选项
     */
    def submitTask(
                      batchDF: DataFrame,
                      hudiTableName: String,
                      hudiTablePath: String,
                      taskId: String = null,
                      options: Map[String, String] = Map.empty
                  ): Unit = {
        val finalTaskId = if (taskId != null && taskId.nonEmpty) taskId else generateTaskId(hudiTableName)
        val task = WriteTask(batchDF, hudiTableName, hudiTablePath, finalTaskId, options)
        taskQueue.addTask(task)
    }

    /**
     * 批量提交写入任务
     *
     * @param tasks 写入任务列表
     */
    def submitTasks(tasks: Seq[WriteTask]): Unit = {
        taskQueue.addTasks(tasks)
    }

    /**
     * 执行所有待处理的写入任务（阻塞直到全部完成）
     *
     * @return 所有任务的执行结果
     */
    def executeAllTasks(): Seq[WriteResult] = {
        val tasks = taskQueue.pollAllTasks()

        if (tasks.isEmpty) {
            logger.warn("没有待处理的写入任务")
            return Seq.empty
        }

        logger.info(s"开始执行 ${tasks.length} 个写入任务，最大并发数: $maxConcurrency")

        // 创建Future任务列表
        val futures: Seq[Future[WriteResult]] = tasks.map { task =>
            Future {
                executor.execute(task)
            }
        }

        // 等待所有任务完成
        val allResults = try {
            val timeout = timeoutSeconds.seconds
            Await.result(Future.sequence(futures), timeout)
        } catch {
            case _: TimeoutException =>
                val errorMsg = s"写入任务执行超时（${timeoutSeconds}秒）"
                logger.error(errorMsg)
                throw new RuntimeException(errorMsg)
            case ex: Exception =>
                logger.error("写入任务执行过程中发生异常", ex)
                throw ex
        }

        // 分析结果
        val successCount = allResults.count(_.isSuccess)
        val failureCount = allResults.count(_.isFailure)

        logger.info(s"写入任务执行完成 - 成功: $successCount, 失败: $failureCount")

        // 如果启用快速失败，检查是否有失败的任务
        if (failFast && failureCount > 0) {
            val failures = allResults.filter(_.isFailure)
            val errorDetails = failures.map(r => s"${r.hudiTableName}: ${r.errorMsg.getOrElse("未知错误")}").mkString("; ")
            val errorMsg = s"写入任务失败（快速失败模式）- 失败任务: $failureCount, 详情: $errorDetails"
            logger.error(errorMsg)
            throw new RuntimeException(errorMsg)
        }

        allResults
    }

    /**
     * 一次性提交并执行多个写入任务
     *
     * @param tasks 写入任务列表
     * @return 所有任务的执行结果
     */
    def submitAndExecute(tasks: Seq[WriteTask]): Seq[WriteResult] = {
        submitTasks(tasks)
        executeAllTasks()
    }

    /**
     * 简化的并发写入接口
     *
     * @param writes 写入参数列表，每个元素包含(DataFrame, 表名, 路径, 选项)
     * @return 所有任务的执行结果
     */
    def concurrentWrite(writes: Seq[(DataFrame, String, String, Map[String, String])]): Seq[WriteResult] = {
        val tasks = writes.zipWithIndex.map { case ((df, tableName, path, options), idx) =>
            WriteTask(
                batchDF = df,
                hudiTableName = tableName,
                hudiTablePath = path,
                taskId = s"task_${tableName}_${idx}_${System.currentTimeMillis()}",
                options = options
            )
        }
        submitAndExecute(tasks)
    }

    /**
     * 获取当前队列状态
     *
     * @return (队列大小, 线程池活跃线程数, 线程池队列大小)
     */
    def getStatus(): (Int, Int, Int) = {
        (taskQueue.size(), threadPool.getActiveCount, threadPool.getQueue.size())
    }

    /**
     * 关闭写入器，释放资源
     */
    def shutdown(): Unit = {
        logger.info("正在关闭HudiConcurrentWriter...")
        threadPool.shutdown()
        try {
            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                threadPool.shutdownNow()
                if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                    logger.warn("线程池无法正常关闭")
                }
            }
        } catch {
            case ex: InterruptedException =>
                threadPool.shutdownNow()
                Thread.currentThread().interrupt()
                logger.warn("线程池关闭过程中被中断", ex)
        }
        logger.info("HudiConcurrentWriter已关闭")
    }

    /**
     * 生成任务ID
     *
     * @param tableName 表名
     * @return 任务ID
     */
    private def generateTaskId(tableName: String): String = {
        s"${tableName}_${System.currentTimeMillis()}_${Thread.currentThread().getId}"
    }
}

/**
 * HudiConcurrentWriter伴生对象
 */
object HudiConcurrentWriter {

    /**
     * 创建默认配置的HudiConcurrentWriter实例
     *
     * @param spark SparkSession实例
     * @return HudiConcurrentWriter实例
     */
    def apply(spark: SparkSession): HudiConcurrentWriter = {
        new HudiConcurrentWriter(spark)
    }

    /**
     * 创建自定义配置的HudiConcurrentWriter实例
     *
     * @param spark          SparkSession实例
     * @param maxConcurrency 最大并发数
     * @param timeoutSeconds 超时时间（秒）
     * @param failFast       是否快速失败
     * @return HudiConcurrentWriter实例
     */
    def apply(
                 spark: SparkSession,
                 maxConcurrency: Int,
                 timeoutSeconds: Long,
                 failFast: Boolean
             ): HudiConcurrentWriter = {
        new HudiConcurrentWriter(spark, maxConcurrency, timeoutSeconds, failFast)
    }

    /**
     * 创建配置的HudiConcurrentWriter实例
     *
     * @param spark  SparkSession实例
     * @param config 配置参数
     * @return HudiConcurrentWriter实例
     */
    def withConfig(spark: SparkSession, config: HudiConcurrentWriterConfig): HudiConcurrentWriter = {
        new HudiConcurrentWriter(spark, config.maxConcurrency, config.timeoutSeconds, config.failFast)
    }
}

/**
 * HudiConcurrentWriter配置类
 *
 * @param maxConcurrency 最大并发数
 * @param timeoutSeconds 超时时间（秒）
 * @param failFast       是否快速失败
 */
case class HudiConcurrentWriterConfig(
                                         maxConcurrency: Int = 5,
                                         timeoutSeconds: Long = 300,
                                         failFast: Boolean = true
                                     ) {
    require(maxConcurrency > 0, "最大并发数必须大于0")
    require(timeoutSeconds > 0, "超时时间必须大于0")
} 