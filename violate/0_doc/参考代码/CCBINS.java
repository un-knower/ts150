package siam.dataload.transform.p9data;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import siam.dataload.dao.DataloadJDBCDao;
import siam.dataload.transform.TransBase;

@Service(value = "siam.dataload.transform.p9data.CCBINS")
public class CCBINS extends TransBase {
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	@Autowired
	private DataloadJDBCDao jdbc;

	@Override
	public void transform(Map<String, Object> fileStatusEntity) {
		log.info("systemtest---- {}", "p9CCBINS transform start");
		//altered by liqian for changing sequence of exec,2013/10/13
		//addHrDepFromEtlCcbins();
		//updateHrDepFromEtlCcbins();
		/**
		 * 查找总行机构号
		 * added by zhangmeina, 2015-07-22
		 */
		String parentBrhNo = getPraentBrhNo();  
		mergeHrDepFromEtlCcbins();
		updateHrDepFromEtlCcbinsRel();
//		updateHrDepFromEtlCcbins();//作废
//		addHrDepFromEtlCcbins();作废
		updateHeadBrhno(parentBrhNo);     //add by yxy 2014-08-20, updated by zhangmeina 2015-07-22
		updateParentBrhno(parentBrhNo);   //add by yxy 2013-12-18, updated by zhangmeina 2015-07-22
		updateHrDepLevel(parentBrhNo);//updated by zhangmeina 2015-07-22
		genDimDept();//add by shendy 2014-8-6
//		updateBrhShortname();
		log.info("systemtest---- {}", "p9CCBINS transform end");
	}

	/**
	 * 查找总行机构号
	 * @author by zhangmeina 2015-07-22
	 * @return
	 */
	private String getPraentBrhNo() {
		String parentBrhNo = "";
		log.info("-------------查找总行机构号--------------------");
		String sql = "select para_value from com_syspara where para_ctg='0003' and para_id = '0001' and para_idx = '0001'";
		SqlRowSet rs = jdbc.getJdbcTemplate().queryForRowSet(sql);
		if (rs.next()) {
			parentBrhNo = rs.getObject(rs.findColumn("para_value")).toString();
		}
		return parentBrhNo;
	}

	/**
	 * 更新机构层级
	 * @Author by 陈刚
	 * updated by zhangmeina 2015-07-22
	 */
	protected void updateHrDepLevel(String brhNo) {
		log.info("-------------更新机构层级:开始--------------------");
		String sql = "update hr_dep set brh_level=0";
		log.info("初始化所有层级为0: " + sql);
		jdbc.execute(sql);

		sql = "update hr_dep set brh_level=1 where brh_no= '" + brhNo+"'";
		log.info(sql);
		int update = jdbc.getJdbcTemplate().update(sql);
		log.info("1级结点,数量: " + update);

		for (int i = 1; i < 20; i++) {
			sql = "update hr_dep set brh_level=" + (i + 1) + " where brh_level=0 and parent_brhno in " +
					" (select brh_no from hr_dep where brh_level=" + i + ")";
			log.info(sql);
			update = jdbc.getJdbcTemplate().update(sql);
			log.info((i + 1) + "级结点,数量: " + update);
			if (update == 0)
				break;
		}

		List<Map<String, Object>> maps = jdbc.getJdbcTemplate().queryForList("select brh_shortname,brh_no,parent_brhno,brh_level from HR_DEP where brh_level=0");
		log.info("未处理(brh_level依然为0)记录数: " + maps.size());
		for (Map<String, Object> map : maps) {
//			System.out.println(map);
		}
		log.info("------------更新机构层级:结束---------------------");
	}

	//add by shendy 2014-8-6
	/**
	 * 生成机构维度表 dim_dep
	 * @Title: genDimDept
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author Think
	 * @date  2015年11月23日 上午10:01:57
	 */
	private void genDimDept(){
		log.info("start gen dimdept");
		jdbc.execute("truncate table dim_dep");
		//1.brh_level=1(总行 111111111)
		jdbc.execute("insert into dim_dep(brh_no,brh_name,brh_shortname,parent_l1,parent_l1_name,parent_l2,parent_l2_name,parent_l3,parent_l3_name,parent_l4,parent_l4_name,parent_l5,parent_l5_name,parent_l6,parent_l6_name,parent_l7,parent_l7_name,parent_l8,parent_l8_name,parent_l9,parent_l9_name,access_path1,access_path2) select brh_no , brh_name,brh_shortname,'-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-',brh_no access_path,'-' from hr_dep where brh_level=1 and not exists (select 1 from dim_dep where hr_dep.brh_no=dim_dep.brh_no)");
		//2.brh_level=2(一级分行)
		jdbc.execute("insert into dim_dep(brh_no,brh_name,brh_shortname,parent_l1,parent_l1_name,parent_l2,parent_l2_name,parent_l3,parent_l3_name,parent_l4,parent_l4_name,parent_l5,parent_l5_name,parent_l6,parent_l6_name,parent_l7,parent_l7_name,parent_l8,parent_l8_name,parent_l9,parent_l9_name,access_path1,access_path2) select hr_dep.brh_no,hr_dep.brh_name,hr_dep.brh_shortname,dim_dep.brh_no parent_l1,dim_dep.brh_shortname parent_l1_name,'-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-',dim_dep.access_path1||'/'||hr_dep.brh_no access_path,'-' from hr_dep,dim_dep where hr_dep.parent_brhno=dim_dep.brh_no and brh_level=2 and not exists (select 1 from dim_dep where hr_dep.brh_no=dim_dep.brh_no)");
		//3.brh_level=3
		jdbc.execute("insert into dim_dep(brh_no,brh_name,brh_shortname,parent_l1,parent_l1_name,parent_l2,parent_l2_name,parent_l3,parent_l3_name,parent_l4,parent_l4_name,parent_l5,parent_l5_name,parent_l6,parent_l6_name,parent_l7,parent_l7_name,parent_l8,parent_l8_name,parent_l9,parent_l9_name,access_path1,access_path2) select hr_dep.brh_no,hr_dep.brh_name,hr_dep.brh_shortname , dim_dep.parent_l1 parent_l1,dim_dep.parent_l1_name parent_l1_name,dim_dep.brh_no parent_l2,dim_dep.brh_shortname parent_l2_name,'-','-','-','-','-','-','-','-','-','-','-','-','-','-',dim_dep.access_path1||'/'||hr_dep.brh_no access_path,'-' from hr_dep,dim_dep where hr_dep.parent_brhno=dim_dep.brh_no and brh_level=3 and not exists (select 1 from dim_dep where hr_dep.brh_no=dim_dep.brh_no)");
		//4.brh_level=4
		jdbc.execute("insert into dim_dep(brh_no,brh_name,brh_shortname,parent_l1,parent_l1_name,parent_l2,parent_l2_name,parent_l3,parent_l3_name,parent_l4,parent_l4_name,parent_l5,parent_l5_name,parent_l6,parent_l6_name,parent_l7,parent_l7_name,parent_l8,parent_l8_name,parent_l9,parent_l9_name,access_path1,access_path2) select hr_dep.brh_no,hr_dep.brh_name,hr_dep.brh_shortname , dim_dep.parent_l1 parent_l1,dim_dep.parent_l1_name parent_l1_name,dim_dep.parent_l2 parent_l2,dim_dep.parent_l2_name parent_l2_name,dim_dep.brh_no parent_l3,dim_dep.brh_shortname parent_l3_name,'-','-','-','-','-','-','-','-','-','-','-','-',dim_dep.access_path1,hr_dep.brh_no access_path from hr_dep,dim_dep where hr_dep.parent_brhno=dim_dep.brh_no and brh_level=4 and not exists (select 1 from dim_dep where hr_dep.brh_no=dim_dep.brh_no)");
		//5.brh_level=5
		jdbc.execute("insert into dim_dep(brh_no,brh_name,brh_shortname,parent_l1,parent_l1_name,parent_l2,parent_l2_name,parent_l3,parent_l3_name,parent_l4,parent_l4_name,parent_l5,parent_l5_name,parent_l6,parent_l6_name,parent_l7,parent_l7_name,parent_l8,parent_l8_name,parent_l9,parent_l9_name,access_path1,access_path2) select hr_dep.brh_no,hr_dep.brh_name,hr_dep.brh_shortname , dim_dep.parent_l1 parent_l1,dim_dep.parent_l1_name parent_l1_name,dim_dep.parent_l2 parent_l2,dim_dep.parent_l2_name parent_l2_name,dim_dep.parent_l3 parent_l3,dim_dep.parent_l3_name parent_l3_name,dim_dep.brh_no parent_l4,dim_dep.brh_shortname parent_l4_name,'-','-','-','-','-','-','-','-','-','-',dim_dep.access_path1, dim_dep.access_path2||'/'||hr_dep.brh_no access_path from hr_dep,dim_dep where hr_dep.parent_brhno=dim_dep.brh_no and brh_level=5 and not exists (select 1 from dim_dep where hr_dep.brh_no=dim_dep.brh_no)");
		//6.brh_level=6
		jdbc.execute("insert into dim_dep(brh_no,brh_name,brh_shortname,parent_l1,parent_l1_name,parent_l2,parent_l2_name,parent_l3,parent_l3_name,parent_l4,parent_l4_name,parent_l5,parent_l5_name,parent_l6,parent_l6_name,parent_l7,parent_l7_name,parent_l8,parent_l8_name,parent_l9,parent_l9_name,access_path1,access_path2) select hr_dep.brh_no,hr_dep.brh_name,hr_dep.brh_shortname , dim_dep.parent_l1 parent_l1,dim_dep.parent_l1_name parent_l1_name,dim_dep.parent_l2 parent_l2,dim_dep.parent_l2_name parent_l2_name,dim_dep.parent_l3 parent_l3,dim_dep.parent_l3_name parent_l3_name,dim_dep.parent_l4 parent_l4,dim_dep.parent_l4_name parent_l4_name ,dim_dep.brh_no parent_l5,dim_dep.brh_shortname parent_l5_name,'-','-','-','-','-','-','-','-',dim_dep.access_path1,dim_dep.access_path2||'/'||hr_dep.brh_no access_path from hr_dep,dim_dep where hr_dep.parent_brhno=dim_dep.brh_no and brh_level=6 and not exists (select 1 from dim_dep where hr_dep.brh_no=dim_dep.brh_no)");
		//7.brh_level=7
		jdbc.execute("insert into dim_dep(brh_no,brh_name,brh_shortname,parent_l1,parent_l1_name,parent_l2,parent_l2_name,parent_l3,parent_l3_name,parent_l4,parent_l4_name,parent_l5,parent_l5_name,parent_l6,parent_l6_name,parent_l7,parent_l7_name,parent_l8,parent_l8_name,parent_l9,parent_l9_name,access_path1,access_path2) select hr_dep.brh_no,hr_dep.brh_name,hr_dep.brh_shortname , dim_dep.parent_l1 parent_l1,dim_dep.parent_l1_name parent_l1_name,dim_dep.parent_l2 parent_l2,dim_dep.parent_l2_name parent_l2_name,dim_dep.parent_l3 parent_l3,dim_dep.parent_l3_name parent_l3_name,dim_dep.parent_l4 parent_l4,dim_dep.parent_l4_name parent_l4_name ,dim_dep.parent_l5 parent_l5,dim_dep.parent_l5_name parent_l5_name,dim_dep.brh_no parent_l6,dim_dep.brh_shortname parent_l6_name,'-','-','-','-','-','-',dim_dep.access_path1,dim_dep.access_path2||'/'||hr_dep.brh_no access_path from hr_dep,dim_dep where hr_dep.parent_brhno=dim_dep.brh_no and brh_level=7 and not exists (select 1 from dim_dep where hr_dep.brh_no=dim_dep.brh_no)");
		
		log.info("end gen dimdept");
	}
	
//	/**
//	 * 删除机构短名称中的无用文字
//	 * @Author by 姚向阳
//	 */
//	private void updateBrhShortname() {
//		try {
//			log.info("-------------删除机构短名称中的无用文字:开始--------------------");
//			String sql = "update hr_dep set brh_shortname=substr(brh_shortname,7) where brh_shortname like '中国建设银行%'";
//			log.info(sql);
//			jdbc.execute(sql);
//
//			sql = "update hr_dep set brh_shortname=replace(brh_shortname,'建设银行','')";
//			log.info(sql);
//			jdbc.execute(sql);
//			
//			sql = "update hr_dep set brh_shortname=replace(brh_shortname,'总行部门直属单位','')";
//			log.info(sql);
//			jdbc.execute(sql);
//			log.info("------------删除机构短名称中的无用文字:结束---------------------");
//		} catch (Exception e) {
//			log.error("删除机构短名称中的无用文字失败");
//		}
//	}

	/**
	 * 历史方法（作废）
	 * 将将P9机构临时表etl_ccbins机构信息和机构关系数据添加到机构部门表Hr_Dep
	 * @Title: addHrDepFromEtlCcbins
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author Think
	 * @date  2015年11月23日 上午9:54:31
	 */
	public void addHrDepFromEtlCcbins() {
		String sql = "insert into Hr_dep(" +
				"brh_no," +
				"brh_pno," +
				"brh_sno," +
				"brh_name," +
				"brh_shortname," +
				"brh_enname," +
				"brh_enshortname," +
				"cd_brhnativelang," +
				"brh_nativename," +
				"parent_brhno," +
				"cd_brhstruct," +
				"brhstruct_desc," +
				"brhlayer_name," +
				"cd_brhlayerstate," +
				"cd_brhrelation," +
				"cd_brhrelationdir," +
				"cd_lifecyclestate," +
				"lifecyclestate_date," +
				"lifecyclestate_desc," +
				"lifecyclestate_id," +
				"cd_mnglevel," +
				"cd_mnglevelval," +
				"mnglevel_desc," +
				"brhmngscn_desc," +
				"brh_attr," +
				"brh_addr," +
				"brh_level," +
				"brh_leader," +
				"secret_adm," +
				"brh_telno," +
				"cd_brhtype," +
				"cd_brhlifestat," +
				"cd_brhchart," +
				"cd_brhlocate," +
				"staff_num," +
				"cd_scale," +
				"stat_weight," +
				"add_user," +
				"add_time," +
				"alt_user," +
				"alt_time," +
				"append1," +
				"append2," +
				"append3)" +

				"select " +
				"e.ccbins_id," +
				"'-'," +
				"'-'," +
				"substr(nvl(e.ccbins_chn_fullnm,'-'),1,50)," +
				"case when e.ccbins_chn_shrtnm like '中国建设银行股份有限公司总行部门直属单位%' then substr(e.ccbins_chn_shrtnm,21,20) when e.ccbins_chn_shrtnm like '中国建设银行股份有限公司%' then substr(e.ccbins_chn_shrtnm,13,20) when e.ccbins_chn_shrtnm like '中国建设银行总行部门直属单位%' then substr(e.ccbins_chn_shrtnm,15,20) when e.ccbins_chn_shrtnm like '中国建设银行%' then substr(e.ccbins_chn_shrtnm,7,20) when e.ccbins_chn_shrtnm like '建设银行总行部门直属单位%' then substr(e.ccbins_chn_shrtnm,13,20) when e.ccbins_chn_shrtnm like '建设银行%' then substr(e.ccbins_chn_shrtnm,5,20) when e.ccbins_chn_shrtnm like '建行%' then substr(e.ccbins_chn_shrtnm,3,20) when e.ccbins_chn_shrtnm like '股份有限公司%' then substr(e.ccbins_chn_shrtnm,7,20) when e.ccbins_chn_shrtnm like '总行部门直属单位%' then substr(e.ccbins_chn_shrtnm,9,20) else substr(e.ccbins_chn_shrtnm,1,20) end," +
				"substr(e.ccbins_eng_fullnm,1,50)," +
				"substr(e.ccbins_eng_shrtnm,1,30)," +
				"'0'||e.ccbins_nm_lng_cd," +
				"e.ccbinsothrlngfull_nm," +
				"'-'," +
				"cc.tcode," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
				"null," +
				"'-'," +
				"null," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
				"nvl(e.entr_lvl_cd,0)," +
				"e.inst_pnp_id," +
				"'-'," +
				"substr(e.fix_telno,1,20)," +
				"'0000'," +
				"'0000'," +
				"'0000'," +
				"'0000'," +
				"e.ccbins_tot_pnum," +
				"'-'," +    //"'00'||e.entp_sz_cd," +
				"0," +
				"'-'," +
				"nvl(e.last_udt_dt_tm,current_date)," +
				"'-'," +
				"current_timestamp," +
				"'-'," +
				"'-'," +
				"0 " +
				"from etl_ccbins_inf e join com_cdmap cc on e.ccbins_tpcd=cc.src_code and cc.map_ctg='0007' " +
				"where not exists " +
				"	(select 1 from hr_dep h where h.brh_no = e.ccbins_id )";
		log.info("p9 CCBINS sql1 is: {}" + sql);
		jdbc.execute(sql);

		sql = "update hr_dep h set(" +
				"parent_brhno," +
				"cd_brhrelation," +
				"cd_brhrelationdir," +
				"cd_lifecyclestate," +
				"lifecyclestate_date," +
				"lifecyclestate_desc," +
				"lifecyclestate_id," +
				"cd_mnglevel," +
				"cd_mnglevelval," +
				"mnglevel_desc," +
				"brhmngscn_desc)=" +
				"(select inst_pid," +
				"substr(rel_ty_cd,1,4)," +
				"rel_from_to_dsc," +
				"rel_lifecycle_st_cd," +
				"rel_lifecycle_st_dt," +
				"rel_lifecycle_st_dsc," +
				"rel_lifecycle_id," +
				"substr(INSTMGTGRD_TPCD,1,4)," +
				"substr(instmgtgrd_tp_val_cd,1,4)," +
				"instmgtgrd_dsc," +
				"rltnp_scn_dsc " +
				"from etl_ccbins_rel e where e.inst_cid = h.brh_no and e.inst_internal_structure_cd = '06') " +
				"where exists " +
				"(select 1 from etl_ccbins_rel where inst_cid = brh_no and inst_internal_structure_cd = '06')";
		log.info("p9 CCBINS sql2 is: {}" + sql);
		jdbc.execute(sql);
	}

	/**
	 * 历史方法（作废）
	 * 将将P9机构临时表etl_ccbins机构信息和机构关系数据更新到机构部门表Hr_Dep
	 * @Title: updateHrDepFromEtlCcbins
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author Think
	 * @date  2015年11月23日 上午9:42:31
	 */
	public void updateHrDepFromEtlCcbins() {
		String sql = "update Hr_dep h set(" +
				"brh_pno," +
				"brh_sno," +
				"brh_name," +
				"brh_shortname," +
				"brh_enname," +
				"brh_enshortname," +
				"cd_brhnativelang," +
				"brh_nativename," +
//				"parent_brhno," +
				"cd_brhstruct," +
				"brhstruct_desc," +
				"brhlayer_name," +
				"cd_brhlayerstate," +
				"cd_brhrelation," +
				"cd_brhrelationdir," +
				"cd_lifecyclestate," +
				"lifecyclestate_date," +
				"lifecyclestate_desc," +
				"lifecyclestate_id," +
				"cd_mnglevel," +
				"cd_mnglevelval," +
				"mnglevel_desc," +
				"brhmngscn_desc," +
				"brh_attr," +
				"brh_addr," +
//				"brh_level," +
				"brh_leader," +
				"secret_adm," +
				"brh_telno," +
				"cd_brhtype," +
				"cd_brhlifestat," +
				"cd_brhchart," +
				"cd_brhlocate," +
				"staff_num," +
//				"cd_scale," +
//				"stat_weight," +
//				"add_user," +
//				"add_time," +
				"alt_user," +
				"alt_time," +
				"append1," +
				"append2," +
				"append3) =" +
				"(select " +
				"'-'," +
				"'-'," +
				"substr(nvl(e.ccbins_chn_fullnm,'-'),1,50)," +
				"case when e.ccbins_chn_shrtnm like '中国建设银行股份有限公司总行部门直属单位%' then substr(e.ccbins_chn_shrtnm,21,20) when e.ccbins_chn_shrtnm like '中国建设银行股份有限公司%' then substr(e.ccbins_chn_shrtnm,13,20) when e.ccbins_chn_shrtnm like '中国建设银行总行部门直属单位%' then substr(e.ccbins_chn_shrtnm,15,20) when e.ccbins_chn_shrtnm like '中国建设银行%' then substr(e.ccbins_chn_shrtnm,7,20) when e.ccbins_chn_shrtnm like '建设银行总行部门直属单位%' then substr(e.ccbins_chn_shrtnm,13,20) when e.ccbins_chn_shrtnm like '建设银行%' then substr(e.ccbins_chn_shrtnm,5,20) when e.ccbins_chn_shrtnm like '建行%' then substr(e.ccbins_chn_shrtnm,3,20) when e.ccbins_chn_shrtnm like '股份有限公司%' then substr(e.ccbins_chn_shrtnm,7,20) when e.ccbins_chn_shrtnm like '总行部门直属单位%' then substr(e.ccbins_chn_shrtnm,9,20) else substr(e.ccbins_chn_shrtnm,1,20) end," +
				"substr(e.ccbins_eng_fullnm,1,50)," +
				"substr(e.ccbins_eng_shrtnm,1,30)," +
				"'0'||e.ccbins_nm_lng_cd," +
				"e.ccbinsothrlngfull_nm," +
//				"'-'," +
				"cc.tcode," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
				"null," +
				"'-'," +
				"null," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
				"'-'," +
//				"nvl(e.entr_lvl_cd,0)," +
				"e.inst_pnp_id," +
				"'-'," +
				"substr(e.fix_telno,1,20)," +
				"'0000'," +
				"'0000'," +
				"'0000'," +
				"'0000'," +
				"e.ccbins_tot_pnum," +
//				"'00'||e.entp_sz_cd," +
//				"0," +
//				"'-'," +
//				"nvl(e.last_udt_dt_tm,current_date)," +
				"'-'," +
				"current_timestamp," +
				"'-'," +
				"'-'," +
				"0 " +
				"from etl_ccbins_inf e left join com_cdmap cc on e.ccbins_tpcd=cc.src_code and cc.map_ctg='0007' where h.brh_no=e.ccbins_id) " +
				"where exists " +
				"	(select 1 from etl_ccbins_inf e where brh_no = ccbins_id)";
		log.info("p9 CCBINS sql3 is: {}" + sql);
		jdbc.execute(sql);

		sql = "update hr_dep h set(" +
				"parent_brhno," +
				"cd_brhrelation," +
				"cd_brhrelationdir," +
				"cd_lifecyclestate," +
				"lifecyclestate_date," +
				"lifecyclestate_desc," +
				"lifecyclestate_id," +
				"cd_mnglevel," +
				"cd_mnglevelval," +
				"mnglevel_desc," +
				"brhmngscn_desc)=" +
				"(select inst_pid," +
				"substr(rel_ty_cd,1,4)," +
				"rel_from_to_dsc," +
				"rel_lifecycle_st_cd," +
				"rel_lifecycle_st_dt," +
				"rel_lifecycle_st_dsc," +
				"rel_lifecycle_id," +
				"substr(INSTMGTGRD_TPCD,1,4)," +
				"substr(instmgtgrd_tp_val_cd,1,4)," +
				"instmgtgrd_dsc," +
				"rltnp_scn_dsc " +
				"from etl_ccbins_rel e where e.inst_cid = h.brh_no and e.inst_internal_structure_cd = '06') " +
				"where exists " +
				"(select 1 from etl_ccbins_rel where inst_cid = brh_no and inst_internal_structure_cd = '06')";
		log.info("p9 CCBINS sql4 is: {}" + sql);

// disable by liqian for not exec twise,2013/10/13
//		log.info("p9 CCBINS sql4 is: {}"+sql);
//		jdbc.execute(sql);
	}
	
	private String transformShortNameStr(){
	    StringBuffer sb = new StringBuffer();
	    sb.append("regexp_replace(");
	    sb.append("regexp_replace(");
	    sb.append("regexp_replace(");
	    sb.append("regexp_replace(");
	    sb.append("regexp_replace(");
	    sb.append("regexp_replace(");
	    sb.append("regexp_replace(");
	    sb.append("regexp_replace(");
	    sb.append("regexp_replace(");
	    sb.append("e.ccbins_chn_shrtnm,");
	    sb.append("'^中国建设银行股份有限公司总行部门直属单位'),");
	    sb.append("'^中国建设银行股份有限公司'),");
	    sb.append("'^中国建设银行总行部门直属单位'),");
	    sb.append("'^中国建设银行'),");
	    sb.append("'^建设银行总行部门直属单位'),");
	    sb.append("'^建设银行'),");
	    sb.append("'^建行'),");
	    sb.append("'^股份有限公司'),");
	    sb.append("'^总行部门直属单位')");
	    return sb.toString();
	}
	
	/**
	 * @Title: mergeHrDepFromEtlCcbins
	 * @Description: 合并变更机构信息至hr_dep表，存在时修改否则进行新增(不更新父机构信息)
	 * @author Think
	 * @date  2015年11月25日 下午4:59:31
	 */
	public void mergeHrDepFromEtlCcbins() {
	    StringBuffer sb = new StringBuffer();
	    sb.append("merge into hr_dep h                                                                                 ").append("\n");
	    sb.append("using (                                                                                             ").append("\n");
	    sb.append("  select t.*,                                                                                       ").append("\n");
	    sb.append("  a.inst_pid,                                                                                       ").append("\n");
	    sb.append("  a.rel_ty_cd,                                                                                      ").append("\n");
	    sb.append("  a.rel_from_to_dsc,                                                                                ").append("\n");
	    sb.append("  a.rel_lifecycle_st_cd,                                                                            ").append("\n");
	    sb.append("  a.rel_lifecycle_st_dt,                                                                            ").append("\n");
	    sb.append("  a.rel_lifecycle_st_dsc,                                                                           ").append("\n");
	    sb.append("  a.rel_lifecycle_id,                                                                               ").append("\n");
	    sb.append("  a.INSTMGTGRD_TPCD,                                                                                ").append("\n");
	    sb.append("  a.instmgtgrd_tp_val_cd,                                                                           ").append("\n");
	    sb.append("  a.instmgtgrd_dsc,                                                                                 ").append("\n");
	    sb.append("  a.rltnp_scn_dsc,                                                                                  ").append("\n");
	    sb.append("  cc.tcode                                                                                          ").append("\n");
//	    sb.append("  from ETL_CCBINS_INF t                                                                             ").append("\n");
//	    sb.append("  left join ETL_CCBINS_REL a on t.ccbins_id = a.inst_cid and a.inst_internal_structure_cd = '06'    ").append("\n");
	    sb.append("from (                                                                                              ").append("\n");
	    sb.append("  select                                                                                            ").append("\n");
	    sb.append("  t.*,                                                                                              ").append("\n");
	    sb.append("  row_number() over(partition by ccbins_id order by t.last_udt_dt_tm desc) rn                       ").append("\n");
	    sb.append("  from ETL_CCBINS_INF t                                                                             ").append("\n");
	    sb.append(") t                                                                                                 ").append("\n");
	    sb.append("left join (                                                                                         ").append("\n");
	    sb.append("  select                                                                                            ").append("\n");
	    sb.append("  t.*,                                                                                              ").append("\n");
	    sb.append("  row_number() over(partition by inst_cid order by t.last_udt_dt_tm desc) rn1                       ").append("\n");
	    sb.append("  from ETL_CCBINS_REL t                                                                             ").append("\n");
	    sb.append("  where  t.inst_internal_structure_cd = '06'                                                        ").append("\n");
	    sb.append(") a on t.ccbins_id = a.inst_cid and a.rn1 = 1                                                       ").append("\n");
	    sb.append("  left join com_cdmap cc on t.ccbins_tpcd=cc.src_code and cc.map_ctg='0007'                         ").append("\n");
	    sb.append("  where t.rn = 1                                                                                    ").append("\n");
	    sb.append(") e on (h.brh_no = e.ccbins_id)                                                                     ").append("\n");
	    sb.append("when matched then                                                                                   ").append("\n");
	    sb.append("update set                                                                                          ").append("\n");
	    sb.append("  h.brh_pno = '-',                                                                                  ").append("\n");
	    sb.append("  h.brh_sno = '-',                                                                                  ").append("\n");
	    sb.append("  h.brh_name = substr(nvl(e.ccbins_chn_fullnm,'-'),1,50),                                           ").append("\n");
	    sb.append("  h.brh_shortname = "+transformShortNameStr()+",                                                    ").append("\n");
	    sb.append("  h.brh_enname = substr(e.ccbins_eng_fullnm,1,50),                                                  ").append("\n");
	    sb.append("  h.brh_enshortname = substr(e.ccbins_eng_shrtnm,1,30),                                             ").append("\n");
	    sb.append("  h.cd_brhnativelang = '0'||e.ccbins_nm_lng_cd,                                                     ").append("\n");
	    sb.append("  h.brh_nativename = e.ccbinsothrlngfull_nm,                                                        ").append("\n");
	    sb.append("  h.cd_brhstruct = e.tcode,                                                                         ").append("\n");
	    sb.append("  h.brhstruct_desc = '-',                                                                           ").append("\n");
	    sb.append("  h.brhlayer_name = '-',                                                                            ").append("\n");
	    sb.append("  h.cd_brhlayerstate = '-',                                                                         ").append("\n");
	    
	    //采用单独方法更新parent_brhno等信息
//	    sb.append("  h.parent_brhno = nvl(e.inst_pid,'-'),                                                               ").append("\n");
//	    sb.append("  h.cd_brhrelation = substr(e.rel_ty_cd,1,4),                                                         ").append("\n");
//	    sb.append("  h.cd_brhrelationdir = e.rel_from_to_dsc,                                                            ").append("\n");
//	    sb.append("  h.cd_lifecyclestate = trim(rel_lifecycle_st_cd),                                                  ").append("\n");
//	    sb.append("  h.lifecyclestate_date = rel_lifecycle_st_dt,                                                      ").append("\n");
//	    sb.append("  h.lifecyclestate_desc = rel_lifecycle_st_dsc,                                                     ").append("\n");
//	    sb.append("  h.lifecyclestate_id = rel_lifecycle_id,                                                           ").append("\n");
//	    sb.append("  h.cd_mnglevel = substr(inst_mgt_grd_tp_code,1,4),                                                 ").append("\n");
//	    sb.append("  h.cd_mnglevelval = substr(instmgtgrd_tp_val_cd,1,4),                                              ").append("\n");
//	    sb.append("  h.mnglevel_desc = instmgtgrd_dsc,                                                                 ").append("\n");
//	    sb.append("  h.brhmngscn_desc = rltnp_scn_dsc,                                                                 ").append("\n");
	    
	    sb.append("  h.brh_attr = '-',                                                                                 ").append("\n");
	    sb.append("  h.brh_addr = '-',                                                                                 ").append("\n");
//	    sb.append("  h.brh_level = 0,                                                                                  ").append("\n");
	    sb.append("  h.brh_leader = e.inst_pnp_id,                                                                     ").append("\n");
	    sb.append("  h.secret_adm = '-',                                                                               ").append("\n");
	    sb.append("  h.brh_telno = substr(e.fix_telno,1,20),                                                           ").append("\n");
	    sb.append("  h.cd_brhtype = '0000',                                                                            ").append("\n");
	    sb.append("  h.cd_brhlifestat = '0000',                                                                        ").append("\n");
	    sb.append("  h.cd_brhchart = '0000',                                                                           ").append("\n");
	    sb.append("  h.cd_brhlocate = '0000',                                                                          ").append("\n");
	    sb.append("  h.staff_num = e.ccbins_tot_pnum,                                                                  ").append("\n");
//	    sb.append("  h.cd_scale = '00'||e.entp_sz_cd,                                                                  ").append("\n");
//	    sb.append("  h.stat_weight = 0,                                                                                ").append("\n");
//	    sb.append("  h.add_user = '-',                                                                                 ").append("\n");
//	    sb.append("  h.add_time = nvl(e.last_udt_dt_tm,current_date),                                                  ").append("\n");
	    sb.append("  h.alt_user = '-',                                                                                 ").append("\n");
	    sb.append("  h.alt_time = current_timestamp,                                                                   ").append("\n");
	    sb.append("  h.append1 = '-',                                                                                  ").append("\n");
	    sb.append("  h.append2 = '-',                                                                                  ").append("\n");
	    sb.append("  h.append3 = 0                                                                                     ").append("\n");
	    sb.append("when not matched then                                                                               ").append("\n");
	    sb.append("insert (                                                                                            ").append("\n");
	    sb.append("  brh_no,                                                                                           ").append("\n");
	    sb.append("  brh_pno,                                                                                          ").append("\n");
	    sb.append("  brh_sno,                                                                                          ").append("\n");
	    sb.append("  brh_name,                                                                                         ").append("\n");
	    sb.append("  brh_shortname,                                                                                    ").append("\n");
	    sb.append("  brh_enname,                                                                                       ").append("\n");
	    sb.append("  brh_enshortname,                                                                                  ").append("\n");
	    sb.append("  cd_brhnativelang,                                                                                 ").append("\n");
	    sb.append("  brh_nativename,                                                                                   ").append("\n");
	    sb.append("  parent_brhno,                                                                                     ").append("\n");
	    sb.append("  cd_brhstruct,                                                                                     ").append("\n");
	    sb.append("  brhstruct_desc,                                                                                   ").append("\n");
	    sb.append("  brhlayer_name,                                                                                    ").append("\n");
	    sb.append("  cd_brhlayerstate,                                                                                 ").append("\n");
	    sb.append("  cd_brhrelation,                                                                                   ").append("\n");
	    sb.append("  cd_brhrelationdir,                                                                                ").append("\n");
	    sb.append("  cd_lifecyclestate,                                                                                ").append("\n");
	    sb.append("  lifecyclestate_date,                                                                              ").append("\n");
	    sb.append("  lifecyclestate_desc,                                                                              ").append("\n");
	    sb.append("  lifecyclestate_id,                                                                                ").append("\n");
	    sb.append("  cd_mnglevel,                                                                                      ").append("\n");
	    sb.append("  cd_mnglevelval,                                                                                   ").append("\n");
	    sb.append("  mnglevel_desc,                                                                                    ").append("\n");
	    sb.append("  brhmngscn_desc,                                                                                   ").append("\n");
	    sb.append("  brh_attr,                                                                                         ").append("\n");
	    sb.append("  brh_addr,                                                                                         ").append("\n");
	    sb.append("  brh_level,                                                                                        ").append("\n");
	    sb.append("  brh_leader,                                                                                       ").append("\n");
	    sb.append("  secret_adm,                                                                                       ").append("\n");
	    sb.append("  brh_telno,                                                                                        ").append("\n");
	    sb.append("  cd_brhtype,                                                                                       ").append("\n");
	    sb.append("  cd_brhlifestat,                                                                                   ").append("\n");
	    sb.append("  cd_brhchart,                                                                                      ").append("\n");
	    sb.append("  cd_brhlocate,                                                                                     ").append("\n");
	    sb.append("  staff_num,                                                                                        ").append("\n");
	    sb.append("  cd_scale,                                                                                         ").append("\n");
	    sb.append("  stat_weight,                                                                                      ").append("\n");
	    sb.append("  add_user,                                                                                         ").append("\n");
	    sb.append("  add_time,                                                                                         ").append("\n");
	    sb.append("  alt_user,                                                                                         ").append("\n");
	    sb.append("  alt_time,                                                                                         ").append("\n");
	    sb.append("  append1,                                                                                          ").append("\n");
	    sb.append("  append2,                                                                                          ").append("\n");
	    sb.append("  append3                                                                                           ").append("\n");
	    sb.append(") values(                                                                                           ").append("\n");
	    sb.append("  e.ccbins_id,                                                                                      ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  substr(nvl(e.ccbins_chn_fullnm,'-'),1,50),                                                        ").append("\n");
	    sb.append("  "+transformShortNameStr()+",                                                                      ").append("\n");
	    sb.append("  substr(e.ccbins_eng_fullnm,1,50),                                                                 ").append("\n");
	    sb.append("  substr(e.ccbins_eng_shrtnm,1,30),                                                                 ").append("\n");
	    sb.append("  '0'||e.ccbins_nm_lng_cd,                                                                          ").append("\n");
	    sb.append("  e.ccbinsothrlngfull_nm,                                                                           ").append("\n");
	    sb.append("  nvl(e.inst_pid,'-'),                                                                              ").append("\n");
	    sb.append("  e.tcode,                                                                                          ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  substr(rel_ty_cd,1,4),                                                                            ").append("\n");
	    sb.append("  rel_from_to_dsc,                                                                                  ").append("\n");
	    sb.append("  trim(rel_lifecycle_st_cd),                                                                        ").append("\n");
	    sb.append("  rel_lifecycle_st_dt,                                                                              ").append("\n");
	    sb.append("  rel_lifecycle_st_dsc,                                                                             ").append("\n");
	    sb.append("  rel_lifecycle_id,                                                                                 ").append("\n");
	    sb.append("  substr(INSTMGTGRD_TPCD,1,4),                                                                      ").append("\n");
	    sb.append("  substr(instmgtgrd_tp_val_cd,1,4),                                                                 ").append("\n");
	    sb.append("  instmgtgrd_dsc,                                                                                   ").append("\n");
	    sb.append("  rltnp_scn_dsc,                                                                                    ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  0,                                                                                                ").append("\n");
	    sb.append("  e.inst_pnp_id,                                                                                    ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  substr(e.fix_telno,1,20),                                                                         ").append("\n");
	    sb.append("  '0000',                                                                                           ").append("\n");
	    sb.append("  '0000',                                                                                           ").append("\n");
	    sb.append("  '0000',                                                                                           ").append("\n");
	    sb.append("  '0000',                                                                                           ").append("\n");
	    sb.append("  e.ccbins_tot_pnum,                                                                                ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  0,                                                                                                ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  nvl(e.last_udt_dt_tm,current_date),                                                               ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  current_timestamp,                                                                                ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  '-',                                                                                              ").append("\n");
	    sb.append("  0                                                                                                 ").append("\n");
	    sb.append(")                                                                                                   ").append("\n");

	    log.info("-------------更新机构信息:开始--------------------");
	    log.info("p9 CCBINS mergeSql is: \n{}" + sb.toString());
	    jdbc.execute(sb.toString());
	    log.info("-------------更新机构信息:结束--------------------");
	}
	
	/**
	 * 
	 * @Title: updateHrDepFromEtlCcbinsRel
	 * @Description: TODO(更新父机构信息)
	 * @author Think
	 * @date  2016年5月26日 上午11:10:35
	 */
	public void updateHrDepFromEtlCcbinsRel() {
	    StringBuffer sb = new StringBuffer();
	    sb.append("merge into hr_dep h                                                                   ").append("\n");
	    sb.append("using(                                                                                ").append("\n");
	    sb.append("select                                                                                ").append("\n");
	    sb.append("a.inst_cid,                                                                           ").append("\n");
	    sb.append("a.inst_pid,                                                                           ").append("\n");
	    sb.append("a.rel_ty_cd,                                                                          ").append("\n");
	    sb.append("a.rel_from_to_dsc,                                                                    ").append("\n");
	    sb.append("a.rel_lifecycle_st_cd,                                                                ").append("\n");
	    sb.append("a.rel_lifecycle_st_dt,                                                                ").append("\n");
	    sb.append("a.rel_lifecycle_st_dsc,                                                               ").append("\n");
	    sb.append("a.rel_lifecycle_id,                                                                   ").append("\n");
	    sb.append("a.INSTMGTGRD_TPCD,                                                                    ").append("\n");
	    sb.append("a.instmgtgrd_tp_val_cd,                                                               ").append("\n");
	    sb.append("a.instmgtgrd_dsc,                                                                     ").append("\n");
	    sb.append("a.rltnp_scn_dsc                                                                       ").append("\n");
	    sb.append("from (                                                                                ").append("\n");
	    sb.append("select                                                                                ").append("\n");
	    sb.append("t.*,                                                                                  ").append("\n");
	    sb.append("row_number() over(partition by inst_cid order by t.last_udt_dt_tm desc) rn1           ").append("\n");
	    sb.append("from ETL_CCBINS_REL t                                                                 ").append("\n");
	    sb.append("where  t.inst_internal_structure_cd = '06'                                            ").append("\n");
	    sb.append(") a where a.rn1 = 1                                                                   ").append("\n");
	    sb.append(") e on (h.brh_no = e.inst_cid)                                                        ").append("\n");
	    sb.append("when matched then                                                                     ").append("\n");
	    sb.append("update set                                                                            ").append("\n");
	    sb.append("h.parent_brhno = nvl(e.inst_pid,'-'),                                                 ").append("\n");
	    sb.append("h.cd_brhrelation = substr(e.rel_ty_cd,1,4),                                           ").append("\n");
	    sb.append("h.cd_brhrelationdir = e.rel_from_to_dsc,                                              ").append("\n");
	    sb.append("h.cd_lifecyclestate = trim(rel_lifecycle_st_cd),                                      ").append("\n");
	    sb.append("h.lifecyclestate_date = rel_lifecycle_st_dt,                                          ").append("\n");
	    sb.append("h.lifecyclestate_desc = rel_lifecycle_st_dsc,                                         ").append("\n");
	    sb.append("h.lifecyclestate_id = rel_lifecycle_id,                                               ").append("\n");
	    sb.append("h.cd_mnglevel = substr(INSTMGTGRD_TPCD,1,4),                                          ").append("\n");
	    sb.append("h.cd_mnglevelval = substr(instmgtgrd_tp_val_cd,1,4),                                  ").append("\n");
	    sb.append("h.mnglevel_desc = instmgtgrd_dsc,                                                     ").append("\n");
	    sb.append("h.brhmngscn_desc = rltnp_scn_dsc                                                      ").append("\n");
	    
	    log.info("-------------更新父机构信息:开始--------------------");
	    log.info("p9 CCBINS_rel mergeSql is: \n{}" + sb.toString());
	    jdbc.execute(sb.toString());
	    log.info("-------------更新父机构信息:结束--------------------");
	}

	/**
	 * 将上级机构为111111111本机构不是111111111且机构号是010或011开头的且本机构不是011000000的机构的上级机构修改为011000000
	 * @Title: updateParentBrhno
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author Think
	 * @date  2015年11月23日 上午9:56:33
	 * @param parentBrhNo
	 */
	public void updateParentBrhno(String parentBrhNo) {
		log.info("-------------调整总行下的一级子机构:开始--------------------");
		String sql = "update hr_dep set parent_brhno='710000000' where parent_brhno ='" + parentBrhNo+ "' and brh_no!='710000000' and (brh_no like '710%')";
		log.info(sql);
		jdbc.execute(sql);

		sql = "update hr_dep set parent_brhno='011000000' where parent_brhno = '"+parentBrhNo + "' and cd_brhstruct != '0001' " +
				" and brh_no != '" + parentBrhNo + "' and brh_no !='011000000' and brh_no !='020000000' and brh_no !='030000000' ";
		log.info(sql);
		jdbc.execute(sql);

		sql = "update hr_dep set parent_brhno='011000000' where parent_brhno = '" + parentBrhNo +"' "+
				" and brh_no != '" + parentBrhNo + "' and brh_no !='011000000' and (brh_no like '010%' or brh_no like '011%') ";
		log.info(sql);
		jdbc.execute(sql);
		
		//将总行机构的层级结构修改为0000（总行机构）
		sql = "update hr_dep set cd_brhstruct='0000',brh_shortname='总行' where brh_no = '011000000' ";
		log.info(sql);
		jdbc.execute(sql);
		
		//将总行下属机构的层级结构修改为0006（部门）
		//除了开发中心和数据中心， updated by zhangmeina 20161222
		sql = "update hr_dep set cd_brhstruct='0006' where parent_brhno = '011000000' and brh_no not in (select a.code from com_syscode a where a.category = '0192')";
		log.info(sql);
		jdbc.execute(sql);
		log.info("-------------调整总行下的一级子机构:结束--------------------");
	}
	
	/**
	 * 更新总行(机构号111111111)的短名称为"中国建设银行股份有限公司"
	 * @Title: updateHeadBrhno
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @author Think
	 * @date  2015年11月23日 上午9:56:24
	 * @param brhNo
	 */
	public void updateHeadBrhno(String brhNo){
		log.info("-----------------------总行(机构号111111111)的短名称为:中国建设银行股份有限公司  开始----------------------------------");
		String sql = "update hr_dep set brh_shortname = '中国建设银行股份有限公司' where brh_no = '" + brhNo+"'";
		log.info(sql);
		jdbc.execute(sql);
		log.info("-----------------------总行(机构号111111111)的短名称为:中国建设银行股份有限公司  结束----------------------------------");
	}
}

