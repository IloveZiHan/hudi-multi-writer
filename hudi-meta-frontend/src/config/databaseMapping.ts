/**
 * 数据库映射配置
 * 用于将数据库名称映射为对应的编号，用于生成表ID
 */

export interface DatabaseMapping {
  [key: string]: string;
}

// 数据库名称到编号的映射配置
export const DATABASE_MAPPING: DatabaseMapping = {
  'cmis': 's001',
  'glloans_xf': 's002',
  'fps': 's003',
  'trade': 's004',
  'risk': 's005',
  'config': 's006',
  'gss': 's006',
  'fsc': 's007',
  'custom': 's008',
  'alc': 's009',
  'mgs': 's011',
  'mpc': 's012',
  'sharedb': 's013',
  'ocs': 's014',
  'collate': 's015',
  'oms': 's016',
  'ccs': 's017',
  'amt': 's018',
  'risk_event': 's019',
  'risk_flow': 's020',
  'risk_variable': 's021',
  'mcs': 's101',
  'wxd': 's103',
  'app': 's105',
  'tracker': 's105',
  'ucenter': 's106',
  'jfu': 's107',
  'hdu': 's108',
  'smsmetric': 's108',
  'paperless39': 's109',
  'olauth': 's110',
  'mpay': 's111',
  'mt_tool': 's111',
  'ccms': 's112',
  'ccms_his': 's112',
  'ccms_user': 's112',
  'coll': 's112',
  'secds': 's112',
  'rmb': 's113',
  'ibct': 's114',
  'zhongyuan20180723': 's115',
  'ctidb': 's116',
  'usercenter': 's117',
  'xn_report': 's117',
  'laiye_saas_knowledge': 's118',
  'im_saas_db': 's118',
  'laiye_saas_dm': 's118',
  'laiye_saas_task': 's118',
  'shop': 's119',
  'datahandler': 's119',
  '9fphonesale20180812': 's120',
  'live800_im': 's121',
  'risk_model': 's123',
  'fqz': 's124',
  'zyabs': 's125',
  'crs': 's126',
  'ueco': 's127',
  'mmp': 's128',
  'zabbix': 's129',
  'bdp': 's130',
  'athena': 's131',
  'public': 's132',
  'openapi': 's133',
  'sdata': 's134',
  'ifrs9': 's135',
  'rs2_rc': 's136',
  'config_ku': 's137',
  'report_ku': 's137',
  'report_cs': 's138',
  'msgu': 's139',
  'label': 's140',
  'yyfx': 's141',
  'hcicloud': 's142',
  'aduser': 's143',
  'sms': 's144',
  'kfops_prod': 's145',
  'ivr': 's146',
  'tmk': 's147',
  'apphit': 's148',
  'fqz_20': 's149',
  'ams': 's150',
  'osp_config': 's151',
  'osp_css': 's151',
  'osp_support': 's151',
  'osp_znwh': 's152',
  'osp_znzj': 's152',
  'osp_zxfz': 's152',
  'zhanye': 's153',
  'osp_coll': 's154',
  'dataware': 's155',
  '数仓rta库': 's156',
  'mt_out_gw': 's157',
  'dis': 's158',
  'rta': 's159',
  'etl': 's160',
  'sql_collect_db': 's161',
  'east4_fy': 's162',
  'rhllbb': 's164',
  'ranger': 's165',
  'luban': 's166',
  'wechat': 's166',
  'mt_qnr': 's166',
  'robot': 's167',
  'newfs': 's168',
  'freyr': 's169',
  'iod': 's170',
  'user_center': 's171',
  'osp_vos': 's172',
  'sys_ferrydata': 's173',
  'devops_ci_teamwork': 's174',
  'pmp': 's176',
  'ufp': 's177',
  'osp_radar': 's178',
  'osp_abs': 's179',
  'aml': 's180',
  'model_backtracking': 's181',
  'proto_validate': 's182',
  'bpbm': 's183',
  'fintell_credit': 's184',
  'sophon': 's186',
  'e_record': 's187',
  'chronicle': 's188',
  'awsdb': 's189',
  'lego': 's190',
  'mt_vip': 's191',
  'mt_equity': 's192',
  'yx_team': 's193',
  'yx_sys': 's193',
  'yx_test': 's193',
  'yx_pipeline': 's193',
  'yx_app': 's193',
  'yx_code': 's193',
  'unionuser': 's194',
  'data_governance': 's195',
  'bdp_third_party_test': 's195',
  'scheduler': 's197',
  'azkaban_pub': 's197',
  'azkaban_priv': 's198',
  'cmdb': 's199',
  'eoms_user': 's199',
  'eoms_alarm': 's199',
  'mt_life': 's200',
  'freyr_zx': 's201',
  'mt_offlinedb': 's202',
  'hive': 's203',
  'mt_tool': 's204',
  'indicators_rt': 's206',
  'recommendations': 's207',
  'log_db': 's210',
  'datahandler': 's212',
};

/**
 * 根据数据库名称获取对应的编号
 * @param databaseName 数据库名称
 * @returns 对应的编号，如果未找到则返回原数据库名称
 */
export function getDatabaseCode(databaseName: string): string {
  if (!databaseName) {
    return '';
  }
  
  const normalizedName = databaseName.toLowerCase().trim();
  return DATABASE_MAPPING[normalizedName] || normalizedName;
}

/**
 * 根据数据库名称和表名生成表ID
 * @param databaseName 数据库名称
 * @param tableName 表名
 * @returns 生成的表ID
 */
export function generateTableId(databaseName: string, tableName: string): string {
  if (!databaseName || !tableName) {
    return '';
  }
  
  const databaseCode = getDatabaseCode(databaseName);
  const normalizedTableName = tableName.toLowerCase().trim();
  
  return `${databaseCode}_${normalizedTableName}`;
}

/**
 * 获取所有可用的数据库名称列表（用于下拉选择）
 * @returns 数据库名称数组
 */
export function getAvailableDatabases(): string[] {
  return Object.keys(DATABASE_MAPPING).sort();
} 