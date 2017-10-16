// Decompiled by Jad v1.5.8g. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://www.kpdus.com/jad.html
// Decompiler options: packimports(3) ansi radix(10) lradix(10) 
// Source File Name:   AutoHighConsumeInfoFacadeControllerBean.java

package com.kingdee.eas.scm.hcc.common.app;

import com.kingdee.bos.BOSException;
import com.kingdee.bos.Context;
import com.kingdee.bos.dao.IObjectCollection;
import com.kingdee.bos.dao.ormapping.ObjectUuidPK;
import com.kingdee.bos.metadata.bot.BOTMappingInfo;
import com.kingdee.bos.metadata.bot.app.BOTMappingHelper;
import com.kingdee.bos.metadata.entity.*;
import com.kingdee.bos.metadata.query.util.CompareType;
import com.kingdee.bos.util.BOSObjectType;
import com.kingdee.bos.util.BOSUuid;
import com.kingdee.eas.base.btp.*;
import com.kingdee.eas.basedata.master.material.*;
import com.kingdee.eas.basedata.org.*;
import com.kingdee.eas.basedata.scm.im.inv.*;
import com.kingdee.eas.common.EASBizException;
import com.kingdee.eas.framework.*;
import com.kingdee.eas.hrp.framework.HrpFrameWorkFacadeFactory;
import com.kingdee.eas.hrp.framework.IHrpFrameWorkFacade;
import com.kingdee.eas.hrp.framework.util.ParamUtil;
import com.kingdee.eas.scm.hcc.*;
import com.kingdee.eas.scm.hcc.common.util.PorpertiesHelper;
import com.kingdee.eas.scm.im.inv.*;
import com.kingdee.eas.util.app.ContextUtil;
import com.kingdee.eas.util.app.DbUtil;
import com.kingdee.jdbc.rowset.IRowSet;
import com.kingdee.jdbc.rowset.impl.JdbcRowSet;
import com.kingdee.util.NumericExceptionSubItem;
import com.kingdee.util.db.SQLUtils;
import java.sql.*;
import java.text.*;
import java.util.*;
import org.apache.log4j.Logger;

// Referenced classes of package com.kingdee.eas.scm.hcc.common.app:
//            AbstractAutoHighConsumeInfoFacadeControllerBean

public class AutoHighConsumeInfoFacadeControllerBean extends
		AbstractAutoHighConsumeInfoFacadeControllerBean {

	public AutoHighConsumeInfoFacadeControllerBean() {
		bacodeInventoryMap = new HashMap();
		purReceivalMap = new HashMap();
		adminMaterialMap = new HashMap();
		materialPurchaseMap = new HashMap();
		srcChargeBillCollection = new CoreBillBaseCollection();
		srcNoChargeBillCollection = new CoreBillBaseCollection();
		idList = new ArrayList();
		codeList = new ArrayList();
		isInWareCodeList = new ArrayList();
		cacheParam = new HashMap();
		messageCode2Str = new HashMap();
	}

	protected String _autoCreate(Context ctx) throws BOSException,
			EASBizException {

		logger.debug("_autoCreate");

		if (m_bRun) {
			return "获取收费数据已经在运行中，请等待完成.";

		}

		m_bRun = true;
		StringBuffer resultStr = new StringBuffer();
		try {

			// 处理收费信息
			autoIndividualCodeChargeData(ctx);

			// 处理退费信息
			autoIndividualCodeNoChargeData(ctx);

			// 设置读取标志
			setReadStatus(ctx);

			if (messageCode2Str.size() > 0) {
				Set keys = messageCode2Str.keySet();
				String key;
				for (Iterator i$ = keys.iterator(); i$.hasNext(); resultStr
						.append((String) messageCode2Str.get(key)).append(";"))
					key = (String) i$.next();

			}
			return resultStr.length() != 0 ? resultStr.substring(0, resultStr
					.length() - 1) : resultStr.toString();
		} catch (SQLException e) {
			throw new EASBizException(new NumericExceptionSubItem("ERRORSQL", e
					.getMessage()));
		} finally {
			m_bRun = false;
		}
	}

	private void autoBOTPTransfer(Context ctx,
			CoreBillBaseCollection srcBillCollection) {
		if (srcBillCollection == null || srcBillCollection.size() == 0)
			return;
		String destBosType = (new MaterialReqBillInfo()).getBOSType()
				.toString();
		BOTMappingInfo bopMappingInfo = null;
		try {
			bopMappingInfo = BOTMappingHelper.getMapping(ctx, srcBillCollection
					.getObject(0), destBosType);
			List srcBillColsList = new ArrayList();
			srcBillColsList.add(srcBillCollection);
			List botMappingPkCols = new ArrayList();
			botMappingPkCols.add(new ObjectUuidPK(bopMappingInfo.getId()
					.toString()));
			IBTPManager iBTPManager = BTPManagerFactory.getLocalInstance(ctx);
			BTPTransformResult result = iBTPManager.innerTransform(
					srcBillColsList, destBosType, botMappingPkCols);
			for (int r = 0; r < result.getBills().size(); r++)
				iBTPManager.saveRelations((CoreBillBaseInfo) result.getBills()
						.getObject(r), result.getBOTRelationCollection());

		} catch (Exception e) {
			logger.error("BOTP规则相应规则不存在或者未启动");
		}
	}

	private void setReadStatus(Context ctx) throws BOSException,
			EASBizException, SQLException {
		if (codeList != null && codeList.size() > 0) {
			StringBuffer sql = new StringBuffer();
			sql.append("UPDATE ").append(tableName).append(
					" SET DQBZ = 1 WHERE INDIVIDUALCODE = ? ");
			executeBatchByDirectConnect(sql.toString(), codeList);
		}
		if (isInWareCodeList != null && isInWareCodeList.size() > 0) {
			String sql = (new StringBuilder()).append(" update ").append(
					tableName).append(
					" set IsInware = 1 where INDIVIDUALCODE = ? ").toString();
			executeBatchByDirectConnect(sql, isInWareCodeList);
		}
	}

	private void autoMerchandiseCodeChargeData(Context ctx)
			throws BOSException, EASBizException, SQLException {
		StringBuffer sql = new StringBuffer();
		if (PorpertiesHelper.getIsDirectConnect().equals("1")) {
			sql.append("SELECT \n");
			sql
					.append("HZXX, FHOSPITALNUMBER ,FMAKEADMINORGNUMBER , FDOCTORNAME , \n");
			sql
					.append("EXECADMINORGNUMBER , FHISCHARGECODE , FHISCHARGEAMOUNT , FCHARGEDATE , \n");
			sql.append("QTY , FDQBZ , FID , FBARCODE , FISCHARGE \n");
			sql.append("ROM ").append(tableName).append(" \n");
			sql.append("WHERE FDQBZ = 0  ORDER BY FCHARGEDATE ");
		} else {
			sql.append("SELECT \n");
			sql
					.append("HZXX, FHOSPITALNUMBER ,FMAKEADMINORGID , FDOCTORNAME , \n");
			sql
					.append("EXECADMINORGID , FHISCHARGECODE , FHISCHARGEAMOUNT , FCHARGEDATE , \n");
			sql
					.append("HISPRICE , FQTY , FREADSTATUS , FID , FBARCODE , FISCHARGE \n");
			sql.append("FROM ").append(tableName).append(" \n");
			sql
					.append("WHERE FREADSTATUS = 0 AND FISINDIVIDUALCODE = 1 ORDER BY FCHARGEDATE ");
		}
		IRowSet rowSet = null;
		if (PorpertiesHelper.getIsDirectConnect().equals("1"))
			rowSet = executeQueryByDirectConnect(sql.toString(), null);
		else
			rowSet = DbUtil.executeQuery(ctx, sql.toString());
		String barcode = null;
		CoreBaseCollection collection = new CoreBaseCollection();
		MaterialImformationInfo imformationInfo;
		for (; rowSet.next(); collection.add(imformationInfo)) {
			barcode = rowSet.getString("BARCODE");
			idList.add(new String[] { rowSet.getString("FID") });
			imformationInfo = new MaterialImformationInfo();
			imformationInfo.setCU(ContextUtil.getCurrentCtrlUnit(ctx));
			MaterialImforEntryInfo entryInfo = new MaterialImforEntryInfo();
			initEntryInfo(entryInfo, rowSet, ctx);
			entryInfo.setIsCharge(ChargeDataTypeEnum.getEnum(Integer
					.parseInt(rowSet.getString("FISCHARGE"))));
			entryInfo.setSpCode(barcode);
			AdminOrgUnitMaterialInfo adminOrgUnitMaterialInfo = getAdminorgUntiMaterialInfo(
					ctx, rowSet.getString("FMAKEADMINORGNUMBER"), barcode);
			if (adminOrgUnitMaterialInfo != null) {
				imformationInfo.setStorageOrg(adminOrgUnitMaterialInfo
						.getStorageUnit());
				MaterialPurchasingInfo materialPurchasingInfo = null;
				if (materialPurchaseMap.get(adminOrgUnitMaterialInfo
						.getStorageUnit().getId().toString()) != null) {
					materialPurchasingInfo = (MaterialPurchasingInfo) materialPurchaseMap
							.get(adminOrgUnitMaterialInfo.getStorageUnit()
									.getId().toString());
				} else {
					Set set = HrpFrameWorkFacadeFactory.getLocalInstance(ctx)
							.getDelegateOrg(
									OrgType.Storage,
									adminOrgUnitMaterialInfo.getStorageUnit()
											.getId().toString(),
									OrgType.Purchase);
					if (set.size() > 0) {
						String purchaseId = (String) set.iterator().next();
						MaterialPurchasingCollection materialPurchasingCollection = MaterialPurchasingFactory
								.getLocalInstance(ctx)
								.getMaterialPurchasingCollection(
										(new StringBuilder()).append(
												"where orgUnit.id = '").append(
												purchaseId).append(
												"' and material.id = '")
												.append(
														adminOrgUnitMaterialInfo
																.getMaterial()
																.getId()
																.toString())
												.append("'").toString());
						if (materialPurchasingCollection.size() > 0) {
							materialPurchasingInfo = materialPurchasingCollection
									.get(0);
							materialPurchaseMap.put(adminOrgUnitMaterialInfo
									.getStorageUnit().getId().toString(),
									materialPurchasingInfo);
						} else {
							return;
						}
					} else {
						return;
					}
				}
				entryInfo.setMaterial(adminOrgUnitMaterialInfo.getMaterial());
				entryInfo.setProductLot(null);
				entryInfo.setPrice(materialPurchasingInfo.getPrice());
				entryInfo.setWarehouse(adminOrgUnitMaterialInfo.getWarehouse());
				entryInfo.setSupplier(materialPurchasingInfo.getSupplier());
				entryInfo.setLot(null);
				entryInfo.setUnit(adminOrgUnitMaterialInfo.getMaterial()
						.getBaseUnit());
				entryInfo.setBaseUnit(adminOrgUnitMaterialInfo.getMaterial()
						.getBaseUnit());
			}
			entryInfo.setParent(imformationInfo);
			imformationInfo.getEntry().add(entryInfo);
			if (!getParam(ctx, imformationInfo.getStorageOrg()))
				continue;
			if (ChargeDataTypeEnum.CHARGE.equals(entryInfo.getIsCharge())) {
				if (entryInfo.getWarehouse() != null
						&& WareHousePropEnum.FINAL_WH.equals(entryInfo
								.getWarehouse().getWarehouseProp()))
					srcChargeBillCollection.add(imformationInfo);
				continue;
			}
			if (entryInfo.getWarehouse() != null
					&& WareHousePropEnum.FINAL_WH.equals(entryInfo
							.getWarehouse().getWarehouseProp()))
				srcNoChargeBillCollection.add(imformationInfo);
		}

		if (collection != null && collection.size() > 0) {
			MaterialImformationFactory.getLocalInstance(ctx).saveBatchData(
					collection);
			logger.debug("success");
		}
	}

	private boolean getParam(Context ctx, StorageOrgUnitInfo storageOrgUnitInfo)
			throws EASBizException, BOSException {
		boolean result = false;
		if (storageOrgUnitInfo == null || storageOrgUnitInfo.getId() == null)
			return result;
		if (cacheParam.get(storageOrgUnitInfo.getId()) != null)
			return ((Boolean) cacheParam.get(storageOrgUnitInfo.getId()))
					.booleanValue();
		Object value = ParamUtil.getParam(ctx, "AutoMaterialReqBill",
				storageOrgUnitInfo);
		if (value == null || value.toString().trim().length() == 0)
			result = false;
		else if ("true".equals(value) || "Y".equals(value))
			result = true;
		cacheParam.put(storageOrgUnitInfo.getId(), Boolean.valueOf(result));
		return result;
	}

	private void initEntryInfo(MaterialImforEntryInfo entryInfo,
			IRowSet rowSet, Context ctx) throws SQLException {
		try {
			if (rowSet.getString("FMAKEADMINORGNUMBER") != null) {
				CostCenterOrgUnitInfo kdcost = new CostCenterOrgUnitInfo();
				String makeNumber = rowSet.getString("FMAKEADMINORGNUMBER");
				String sql = (new StringBuilder())
						.append(
								"select CFOFFICENUMID from CT_CUS_SysControlEntry where CFHISOFFICENUM='")
						.append(makeNumber).append("'").toString();
				try {
					IRowSet set = DbUtil.executeQuery(ctx, sql);
					if (null != set)
						if (set.next())
							kdcost.setId(BOSUuid.read(set
									.getString("CFOFFICENUMID")));
						else
							messageCode2Str.put(entryInfo.getIndividualCode(),
									(new StringBuilder()).append("没有找到编码为")
											.append(makeNumber).append("的开单科室")
											.toString());
				} catch (BOSException e) {
					e.printStackTrace();
				}
				entryInfo.setMakeAdminOrg(kdcost);
			}
		} catch (Exception e) {
			messageCode2Str.put(entryInfo.getIndividualCode(),
					(new StringBuilder()).append("收费信息中间库中个体码").append(
							entryInfo.getIndividualCode()).append("没有设置开单科室")
							.toString());
		}
	}

	private AdminOrgUnitMaterialInfo getAdminorgUntiMaterialInfo(Context ctx,
			String zxdeptid, String barcode) throws BOSException {
		if (adminMaterialMap.get((new StringBuilder()).append(zxdeptid).append(
				barcode).toString()) != null)
			return (AdminOrgUnitMaterialInfo) adminMaterialMap
					.get((new StringBuilder()).append(zxdeptid).append(barcode)
							.toString());
		EntityViewInfo view = new EntityViewInfo();
		FilterInfo fitlerIfno = new FilterInfo();
		fitlerIfno.getFilterItems().add(
				new FilterItemInfo("material.barcode", barcode,
						CompareType.EQUALS));
		fitlerIfno.getFilterItems().add(
				new FilterItemInfo("adminOrgUnit.number", zxdeptid,
						CompareType.EQUALS));
		view.setFilter(fitlerIfno);
		SelectorItemCollection sic = new SelectorItemCollection();
		sic.add(new SelectorItemInfo("*"));
		sic.add(new SelectorItemInfo("adminOrgUnit.*"));
		sic.add(new SelectorItemInfo("material.*"));
		sic.add(new SelectorItemInfo("material.baseUnit.*"));
		sic.add(new SelectorItemInfo("storageOrgUnit.*"));
		sic.add(new SelectorItemInfo("warehouse.*"));
		view.setSelector(sic);
		AdminOrgUnitMaterialCollection adminOrgUnitMaterialCollection = AdminOrgUnitMaterialFactory
				.getLocalInstance(ctx).getAdminOrgUnitMaterialCollection(view);
		if (adminOrgUnitMaterialCollection.size() > 0) {
			adminMaterialMap.put((new StringBuilder()).append(zxdeptid).append(
					barcode).toString(), adminOrgUnitMaterialCollection.get(0));
			return adminOrgUnitMaterialCollection.get(0);
		} else {
			return null;
		}
	}

	/**
	 * 退费信息生成
	 * 
	 * @param ctx
	 * @throws BOSException
	 * @throws EASBizException
	 * @throws SQLException
	 */
	private void autoIndividualCodeNoChargeData(Context ctx)
			throws BOSException, EASBizException, SQLException {

		IRowSet rowSet = getIndividualCodeList(ctx, false);
		String barcode = null;
		Set deleteSet = new HashSet();
		while (rowSet.next()) {
			barcode = rowSet.getString("INDIVIDUALCODE");
			if (!isExistBarcode(ctx, barcode))
				logger.error((new StringBuilder()).append(barcode).append(
						"该个体码收费数据不存在").toString());
			else
				try {

					synchMaterialUsedInfo(ctx, barcode);
					deleteSet.add(barcode);
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
		}

		// 删除传递过来的收费数据
		if (deleteSet != null && deleteSet.size() > 0) {
			FilterInfo deleteFilter = new FilterInfo();
			deleteFilter.getFilterItems().add(
					new FilterItemInfo("individualCode", deleteSet,
							CompareType.INCLUDE));
			deleteFilter.getFilterItems().add(
					new FilterItemInfo("isNormalOut", Integer.valueOf(0)));
			deleteFilter.getFilterItems().add(
					new FilterItemInfo("isConsignOut", Integer.valueOf(0)));
			deleteFilter.getFilterItems().add(
					new FilterItemInfo("isPurInware", Integer.valueOf(0)));
			MaterialImforEntryFactory.getLocalInstance(ctx)
					.delete(deleteFilter);
		}
	}

	/***
	 * 检查是否已经生成出库单，生成则不允许进行退费操作（删除收费信息表）
	 * 
	 * @param ctx
	 * @param barcode
	 * @throws Exception
	 */
	private void synchMaterialUsedInfo(Context ctx, String barcode)
			throws Exception {
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT FISNORMALOUT , FISCONSIGNOUT , FISPURINWARE \n ");
		sql.append("FROM T_HCC_MATERIALIMFORENTRY WHERE \n");
		sql.append("(FISNORMALOUT = 1 \n");
		sql.append("OR FISCONSIGNOUT = 1 \n");
		sql.append("OR FISPURINWARE = 1 )\n");
		sql.append("AND FINDIVIDUALCODE = ? ");
		Object params[] = { barcode };
		IRowSet rowSet = DbUtil.executeQuery(ctx, sql.toString(), params);
		if (rowSet.next()) {
			StringBuffer message = new StringBuffer();
			message.append("个体码：").append(barcode);
			if (rowSet.getInt("FISNORMALOUT") == 1)
				message.append("已经生成").append("普通领料出库单").append("\n");
			if (rowSet.getInt("FISCONSIGNOUT") == 1)
				message.append("已经生成").append("代销领料出库单").append("\n");
			if (rowSet.getInt("FISPURINWARE") == 1)
				message.append("已经生成").append("采购入库单").append("\n");
			message.append("不允许退费");
			throw new Exception(message.toString());
		} else {
			return;
		}
	}

	/***
	 * 获取个体码信息
	 * 
	 * @param ctx
	 * @param flag
	 *            ：0true:true:收费/false:退费
	 * @return
	 * @throws BOSException
	 * @throws EASBizException
	 * @throws SQLException
	 */
	private IRowSet getIndividualCodeList(Context ctx, boolean flag)
			throws BOSException, EASBizException, SQLException {
		StringBuffer sql = new StringBuffer();
		if (PorpertiesHelper.getIsDirectConnect().equals("1")) {
			sql.append("SELECT ");
			sql.append("HZXX, ");
			sql.append("HOSPITALNUMBER, ");
			sql.append("ZYCS, ");
			sql.append("SYXH, ");
			sql.append("INDIVIDUALCODE,");
			sql.append("MAKEADMINORGNUMBER,");
			sql.append("MAKEADMINORGNAME,");
			sql.append("DOCTORNUMBER,");
			sql.append("DOCTORNAME,");
			sql.append("CHARGERNUMBER,");
			sql.append("CHARGERNAME,");
			sql.append("HISCHARGECODE,");
			sql.append("SFXMMC,");
			sql.append("SFDJ,");
			sql.append("HISCHARGEAMOUNT,");
			sql.append("QTY,");
			sql.append("SFRQ,");
			sql.append("JLZT,");
			sql.append("ISINWARE,");
			sql.append("DQBZ,");
			sql.append("DQSJ ");
			sql.append(" FROM ");
			sql.append(tableName);
			sql.append(" WHERE 1= 1 AND JLZT = ? AND DQBZ = 0 ORDER BY SFRQ ");
		} else {
			sql.append("SELECT \n");
			sql
					.append("FHZXX, FHOSPITALNUMBER ,FMAKEADMINORGID , FDOCTORNAME , \n");
			sql
					.append("FEXECADMINORGID , FHISCHARGECODE , FHISCHARGEAMOUNT , FCHARGEDATE , \n");
			sql.append("FHISPRICE , FQTY , FREADSTATUS , FID , FBARCODE \n");
			sql.append("FROM ").append(tableName).append(" \n");
			sql
					.append("WHERE FISCHARGE = ? AND FREADSTATUS = 0 AND FISINDIVIDUALCODE = 0 ORDER BY FCHARGEDATE ");
		}
		int param = 1;
		if (flag)
			param = 0;
		IRowSet rowSet = null;
		if (PorpertiesHelper.getIsDirectConnect().equals("1"))
			rowSet = executeQueryByDirectConnect(sql.toString(),
					new Object[] { Integer.valueOf(param) });
		else
			rowSet = DbUtil.executeQuery(ctx, sql.toString(),
					new Object[] { Integer.valueOf(param) });
		return rowSet;
	}

	public static IRowSet executeQueryByDirectConnect(String sql,
			Object params[]) throws BOSException {
		Connection conn;
		PreparedStatement ps;
		java.sql.ResultSet rs;
		conn = null;
		ps = null;
		rs = null;
		try {
			Class.forName(PorpertiesHelper.getDBDRIVER());
			conn = DriverManager.getConnection(PorpertiesHelper.getDBURL(),
					PorpertiesHelper.getDBNAME(), PorpertiesHelper.getDBPWD());
		} catch (SQLException exc) {
			SQLUtils.cleanup(conn);
			throw new BOSException(CONFIG_EXCEPTION, exc);
		} catch (ClassNotFoundException e) {
			SQLUtils.cleanup(conn);
			throw new BOSException(CONFIG_EXCEPTION, e);
		}
		JdbcRowSet jdbcrowset;
		try {
			ps = conn.prepareStatement(sql);
			if (null != params) {
				for (int i = 0; i < params.length; i++)
					if (params[i] != null)
						ps.setObject(i + 1, params[i]);
					else
						ps.setNull(i + 1, 12);

			}
			rs = ps.executeQuery();
			JdbcRowSet rowset = new JdbcRowSet();
			rowset.populate(rs);
			jdbcrowset = rowset;
			return jdbcrowset;
		} catch (SQLException exc) {
			StringBuffer sb = new StringBuffer("");
			for (int i = 0; i < params.length; i++)
				sb.append("param ").append(i).append(" is:").append(params[i]);

			logger.error((new StringBuilder()).append("sql is:").append(sql)
					.append(" param is:").append(sb.toString()).toString());
			logger.error("sql error!", exc);
			throw new BOSException((new StringBuilder()).append(
					"Sql execute exception : ").append(sql).toString(), exc);
		} finally {
			SQLUtils.cleanup(rs, ps, conn);
		}

	}

	public static boolean executeSqlByDirectConnect(String sql)
			throws BOSException {
		Connection conn;
		PreparedStatement ps;
		java.sql.ResultSet rs;
		conn = null;
		ps = null;
		rs = null;
		try {
			Class.forName(PorpertiesHelper.getDBDRIVER());
			conn = DriverManager.getConnection(PorpertiesHelper.getDBURL(),
					PorpertiesHelper.getDBNAME(), PorpertiesHelper.getDBPWD());
		} catch (SQLException exc) {
			SQLUtils.cleanup(conn);
			throw new BOSException(CONFIG_EXCEPTION, exc);
		} catch (ClassNotFoundException e) {
			SQLUtils.cleanup(conn);
			throw new BOSException(CONFIG_EXCEPTION, e);
		}
		boolean flag;
		try {
			ps = conn.prepareStatement(sql);
			flag = ps.execute();
			return flag;
		} catch (SQLException exc) {
			throw new BOSException((new StringBuilder()).append(
					"Sql execute exception : ").append(sql).toString(), exc);
		} finally {
			SQLUtils.cleanup(rs, ps, conn);

		}

	}

	private Set<String> getExistBarcode(Context ctx, IRowSet rowSet)
			throws SQLException, BOSException {

		Set<String> retSet = new HashSet<String>();

		IRowSet resultRowSet = rowSet;

		String barcodeStr = "";

		StringBuilder sb = new StringBuilder();

		while (rowSet.next()) {

			// 获取所有barcode用于查询条件字符串 eg： 'a','b'
			sb.append("'").append(rowSet.getString("INDIVIDUALCODE")).append(
					"',");

			if (sb.length() > 500) {

				barcodeStr = sb.substring(0, sb.length() - 1);
				// 一次性把所有存在的个体码查询出来
				IRowSet existRowSet = getBarcode(ctx, barcodeStr);

				while (existRowSet.next()) {

					retSet.add(existRowSet.getString("FINDIVIDUALCODE"));
				}

				sb = new StringBuilder();
			}

		}
		// --test---begin---
		if (sb.length()==0){
			sb.append("'").append("12345678").append("','").append("987654321").append("',");
		}
		if (sb.length() > 0) {
			barcodeStr = sb.substring(0, sb.length() - 1);
			// 一次性把所有存在的个体码查询出来
			IRowSet existRowSet = getBarcode(ctx, barcodeStr);

			while (existRowSet.next()) {

				retSet.add(existRowSet.getString("FINDIVIDUALCODE"));
			}

		}
		// --test---end---
		return retSet;
	}

	private Map<String, Object> getMakeNumber(Context ctx, IRowSet rowSet)
			throws SQLException, BOSException {

		Map<String, Object> retMap = new HashMap<String, Object>();

		IRowSet resultRowSet = rowSet;

		String barcodeStr = "";

		StringBuilder sb = new StringBuilder();

		while (rowSet.next()) {

			// 获取所有barcode用于查询条件字符串 eg： 'a','b'
			sb.append("'").append(rowSet.getString("MAKEADMINORGNUMBER"))
					.append("',");

			if (sb.length() > 500) {
				barcodeStr = sb.substring(0, sb.length() - 1);

				String sql = "select CFOFFICENUMID,CFHISOFFICENUM from CT_CUS_SysControlEntry where CFHISOFFICENUM IN ("
						+ barcodeStr + ")";
				// 一次性把所有存在的个体码查询出来
				IRowSet existRowSet = DbUtil.executeQuery(ctx, sql);

				while (existRowSet.next()) {

					retMap.put(existRowSet.getString("CFHISOFFICENUM"),
							existRowSet.getString("CFOFFICENUMID"));
				}

			}

			sb = new StringBuilder();

		}

		// if (sb.length() > 0) {
		// barcodeStr = sb.substring(0, sb.length() - 1);
		//
		// String sql =
		// "select CFOFFICENUMID,CFHISOFFICENUM from CT_CUS_SysControlEntry where CFHISOFFICENUM IN ("
		// + barcodeStr + ")";
		// // 一次性把所有存在的个体码查询出来
		// existRowSet = DbUtil.executeQuery(ctx, sql);
		// }
		//
		// while (existRowSet.next()) {
		//
		// retMap.put(existRowSet.getString("CFHISOFFICENUM"),existRowSet.
		// getString("CFOFFICENUMID"));
		// }

		return retMap;
	}

	/***
	 * 获取计费数据
	 * 
	 * @param ctx
	 * @throws BOSException
	 * @throws EASBizException
	 * @throws SQLException
	 */
	private void autoIndividualCodeChargeData(Context ctx) throws BOSException,
			EASBizException, SQLException {

		IRowSet rowSet = getIndividualCodeList(ctx, true);
		CoreBaseCollection collection = new CoreBaseCollection();

		Set<String> existBarcodeSet = getExistBarcode(ctx, rowSet);

		Map<String, Object> existMakeNumberMap = getMakeNumber(ctx, rowSet);

		while (rowSet.next()) {
			String barcode = rowSet.getString("INDIVIDUALCODE");
			if (null == barcode || "".equals(barcode)) {
				messageCode2Str.put(barcode, "收费信息中间库中个体码没有设置");
				continue;
			}

			if (existBarcodeSet.contains(barcode)) {
				messageCode2Str.put(barcode, (new StringBuilder()).append(
						"收费信息中间库中个体码").append(barcode).append(
						"收费数据已经存在，不允许重复收费").toString());
				continue;
			}

			// if (isExistBarcode(ctx, barcode)) {
			// messageCode2Str.put(barcode, (new StringBuilder()).append(
			// "收费信息中间库中个体码").append(barcode).append(
			// "收费数据已经存在，不允许重复收费").toString());
			// continue;
			// }

			boolean isInware = isInware(ctx, barcode); //todo
			if (isInware) {
				isInWareCodeList.add(new String[] { barcode });
			}

			String syxh = rowSet.getString("SYXH");
			String sfrq = rowSet.getString("SFRQ");
			String zycs = rowSet.getString("ZYCS");
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			format.setLenient(false);
			Timestamp tmSfrq;
			try {
				tmSfrq = new Timestamp(format.parse(sfrq).getTime());
			} catch (ParseException e) {
				messageCode2Str.put(barcode, (new StringBuilder()).append(
						"中间库个体码").append(barcode).append(
						"的收费日期格式不正确，应为yyyy-MM-dd HH:mm:ss").toString());
				continue;
			}

			MaterialImformationInfo imformationInfo = new MaterialImformationInfo();
			imformationInfo.setCU(ContextUtil.getCurrentCtrlUnit(ctx));

			MaterialImforEntryInfo entryInfo = new MaterialImforEntryInfo();

			if (rowSet.getString("MAKEADMINORGNUMBER") == null) {
				messageCode2Str.put(barcode, (new StringBuilder()).append(
						"收费信息中间库中个体码").append(entryInfo.getIndividualCode())
						.append("没有设置开单科室").toString());
				continue;
			}

			CostCenterOrgUnitInfo kdcost = new CostCenterOrgUnitInfo();
			String makeNumber = rowSet.getString("MAKEADMINORGNUMBER");

			if (existMakeNumberMap.containsKey(makeNumber)) {
				kdcost.setId(BOSUuid.read(existMakeNumberMap.get(makeNumber)
						.toString()));
				entryInfo.setMakeAdminOrg(kdcost);
			} else {
				messageCode2Str.put(barcode, (new StringBuilder()).append(
						"没有找到编码为").append(makeNumber).append("的开单科室")
						.toString());
				continue;
			}
			// String sql = (new StringBuilder())
			// .append(
			// "select CFOFFICENUMID from CT_CUS_SysControlEntry where CFHISOFFICENUM='"
			// )
			// .append(makeNumber).append("'").toString();
			// try {
			// IRowSet set = DbUtil.executeQuery(ctx, sql);
			// if (set != null && set.next()) {
			// kdcost.setId(BOSUuid.read(set.getString("CFOFFICENUMID")));
			// entryInfo.setMakeAdminOrg(kdcost);
			//
			// } else {
			// messageCode2Str.put(barcode, (new StringBuilder()).append(
			// "没有找到编码为").append(makeNumber).append("的开单科室")
			// .toString());
			// continue;
			// }
			// } catch (BOSException e) {
			// messageCode2Str.put(barcode, (new StringBuilder()).append(
			// "没有找到编码为").append(makeNumber).append("的开单科室")
			// .toString());
			// continue;
			// }

			entryInfo.setIsCharge(ChargeDataTypeEnum.CHARGE);
			entryInfo.setHzxx(rowSet.getString("HZXX"));
			entryInfo.setIndividualCode(barcode);
			entryInfo.setSyxh(syxh);
			entryInfo.setChargeDate(tmSfrq);
			entryInfo.setZycs(zycs);
			entryInfo.setDoctorNumber(rowSet.getString("DOCTORNUMBER"));
			entryInfo.setDoctorName(rowSet.getString("DOCTORNAME"));
			entryInfo.setChargerNumber(rowSet.getString("CHARGERNUMBER"));
			entryInfo.setChargerName(rowSet.getString("CHARGERNAME"));
			entryInfo.setHisChargeCode(rowSet.getString("HISCHARGECODE"));
			entryInfo.setSfxmmc(rowSet.getString("SFXMMC"));
			entryInfo.setHISPrice(rowSet.getBigDecimal("SFDJ"));
			entryInfo.setHISChargeAmount(rowSet
					.getBigDecimal("HISCHARGEAMOUNT"));
			entryInfo.setIsCharge(ChargeDataTypeEnum.getEnum(rowSet
					.getInt("JLZT")));
			entryInfo.setQty(rowSet.getBigDecimal("QTY"));
			entryInfo.setDoctorName(rowSet.getString("DOCTORNAME"));
			entryInfo.setHospitalNumber(rowSet.getString("HOSPITALNUMBER"));
			entryInfo.setIsNormalOut(false);
			entryInfo.setIsConsignOut(false);
			entryInfo.setIsPurInware(false);
			entryInfo.setIsInware(isInware);
			entryInfo.setIsPurInware(!isGenPurInware(ctx, barcode)); //todo
			BarcodeInventoryInfo barcodeInventoryInfo = null;
			boolean isExist = false;

			if (barcodeInventoryInfo != null) {
				imformationInfo.setStorageOrg(barcodeInventoryInfo
						.getStorageId());
				entryInfo.setMaterial(barcodeInventoryInfo.getMaterialId());
				entryInfo.setProductLot(barcodeInventoryInfo.getProductLot());
				entryInfo.setPrice(barcodeInventoryInfo.getPrice());
				entryInfo.setWarehouse(barcodeInventoryInfo.getWarehouseId());
				entryInfo.setSupplier(barcodeInventoryInfo.getInventoryId()
						.getSupplier());
				entryInfo.setLot(barcodeInventoryInfo.getLot());
				entryInfo.setUnit(barcodeInventoryInfo.getUnitId());
				entryInfo.setBaseUnit(barcodeInventoryInfo.getUnitId());
				isExist = true;
			} else {
				PurReceivalEntryInfo purReceivalEntryInfo = getPurReceivalInfo(
						ctx, barcode);
				if (purReceivalEntryInfo != null) {
					imformationInfo.setStorageOrg(purReceivalEntryInfo
							.getParent().getStorageOrgUnit());
					entryInfo.setMaterial(purReceivalEntryInfo.getMaterial());
					entryInfo.setProductLot(purReceivalEntryInfo.getPlotNum());
					entryInfo
							.setPrice(purReceivalEntryInfo.getUnitActualCost());
					entryInfo.setWarehouse(purReceivalEntryInfo.getWarehouse());
					entryInfo.setSupplier(purReceivalEntryInfo.getParent()
							.getSupplier());
					entryInfo.setLot(purReceivalEntryInfo.getLot());
					entryInfo.setUnit(purReceivalEntryInfo.getUnit());
					entryInfo.setBaseUnit(purReceivalEntryInfo.getBaseUnit());
					isExist = true;
				}
			}

			if (isExist) {
				codeList.add(new String[] { barcode });
				entryInfo.setParent(imformationInfo);
				imformationInfo.getEntry().add(entryInfo);
				collection.add(imformationInfo);
			} else {
				messageCode2Str.put(barcode, (new StringBuilder()).append(
						"收费信息中间库中个体码").append(barcode).append("在采购收货单中不存在")
						.toString());
			}

		}

		if (collection != null && collection.size() > 0)
			MaterialImformationFactory.getLocalInstance(ctx).saveBatchData(
					collection);

	}

	private boolean isExistBarcode(Context ctx, String barcode)
			throws BOSException, SQLException {
		boolean isExist = false;
		String sql = "SELECT FINDIVIDUALCODE FROM T_HCC_MATERIALIMFORENTRY WHERE FINDIVIDUALCODE = ? ";
		Object params[] = { barcode };
		IRowSet rowSet = DbUtil.executeQuery(ctx, sql, params);
		if (rowSet.next())
			isExist = true;
		return isExist;
	}

	/**
	 * 查询barcodes中在T_HCC_MATERIALIMFORENTRY表中中存在的
	 * 
	 * @param ctx
	 * @param barcodes
	 * @return
	 * @throws BOSException
	 * @throws SQLException
	 */
	private IRowSet getBarcode(Context ctx, String barcodes)
			throws BOSException, SQLException {

		String sql = "SELECT FINDIVIDUALCODE FROM T_HCC_MATERIALIMFORENTRY WHERE FINDIVIDUALCODE in ("
				+ barcodes + ") ";
		// Object params[] = { barcodes };
		IRowSet rowSet = DbUtil.executeQuery(ctx, sql);

		return rowSet;
	}

	public BarcodeInventoryInfo getBarcodeInventory(Context ctx, String barcode)
			throws BOSException {
		if (bacodeInventoryMap.get(barcode) != null)
			bacodeInventoryMap.get(barcode);
		EntityViewInfo view = new EntityViewInfo();
		SelectorItemCollection sic = new SelectorItemCollection();
		sic.add(new SelectorItemInfo("*"));
		sic.add(new SelectorItemInfo("storageId.*"));
		sic.add(new SelectorItemInfo("warehouseId.*"));
		sic.add(new SelectorItemInfo("locationId.*"));
		sic.add(new SelectorItemInfo("materialId.*"));
		sic.add(new SelectorItemInfo("unitId.*"));
		sic.add(new SelectorItemInfo("inventoryId.supplier.*"));
		view.setSelector(sic);
		FilterInfo filterInfo = new FilterInfo();
		filterInfo.getFilterItems().add(new FilterItemInfo("barcode", barcode));
		view.setFilter(filterInfo);
		BarcodeInventoryCollection barcodeCollection = BarcodeInventoryFactory
				.getLocalInstance(ctx).getBarcodeInventoryCollection(view);
		if (barcodeCollection.size() > 0) {
			bacodeInventoryMap.put(barcode, barcodeCollection.get(0));
			return barcodeCollection.get(0);
		} else {
			return null;
		}
	}

	public PurReceivalEntryInfo getPurReceivalInfo(Context ctx, String barcode)
			throws BOSException {
		if (purReceivalMap.get(barcode) != null)
			return (PurReceivalEntryInfo) purReceivalMap.get(barcode);
		EntityViewInfo view = new EntityViewInfo();
		FilterInfo fitlerIfno = new FilterInfo();
		fitlerIfno.getFilterItems().add(
				new FilterItemInfo("innercode", barcode));
		fitlerIfno.getFilterItems().add(
				new FilterItemInfo("parent.transactionType.existingQty",
						Integer.valueOf(0)));
		view.setFilter(fitlerIfno);
		SelectorItemCollection sic = new SelectorItemCollection();
		sic.add(new SelectorItemInfo("*"));
		sic.add(new SelectorItemInfo("parent.*"));
		sic.add(new SelectorItemInfo("parent.supplier.*"));
		sic.add(new SelectorItemInfo("parent.storageOrgUnit.*"));
		sic.add(new SelectorItemInfo("material.*"));
		sic.add(new SelectorItemInfo("warehouse.id"));
		view.setSelector(sic);
		PurReceivalEntryCollection purReceivalEntryCollection = PurReceivalEntryFactory
				.getLocalInstance(ctx).getPurReceivalEntryCollection(view);
		if (purReceivalEntryCollection.size() > 0) {
			purReceivalMap.put(barcode, purReceivalEntryCollection.get(0));
			return purReceivalEntryCollection.get(0);
		} else {
			return null;
		}
	}

	public boolean isInware(Context ctx, String barcode) throws BOSException,
			EASBizException {
		EntityViewInfo view = new EntityViewInfo();
		FilterInfo fitlerIfno = new FilterInfo();
		fitlerIfno.getFilterItems().add(
				new FilterItemInfo("hrpBarNumber", barcode));
		fitlerIfno.getFilterItems().add(
				new FilterItemInfo("parent.baseStatus", Integer.valueOf(4)));
		view.setFilter(fitlerIfno);
		return MaterialReqBillEntryFactory.getLocalInstance(ctx).exists(
				fitlerIfno);
	}

	public boolean isGenPurInware(Context ctx, String barcode)
			throws BOSException, EASBizException {
		EntityViewInfo view = new EntityViewInfo();
		FilterInfo fitlerIfno = new FilterInfo();
		fitlerIfno.getFilterItems().add(
				new FilterItemInfo("hrpBarNumber", barcode));
		fitlerIfno.getFilterItems().add(
				new FilterItemInfo("parent.baseStatus", Integer.valueOf(4)));
		view.setFilter(fitlerIfno);
		return PurInWarehsEntryFactory.getLocalInstance(ctx).exists(fitlerIfno);
	}

	public static void executeBatchByDirectConnect(String sql, List paramsList)
			throws BOSException {
		Connection conn;
		PreparedStatement ps;
		conn = null;
		ps = null;
		try {
			Class.forName(PorpertiesHelper.getDBDRIVER());
			conn = DriverManager.getConnection(PorpertiesHelper.getDBURL(),
					PorpertiesHelper.getDBNAME(), PorpertiesHelper.getDBPWD());
			ps = conn.prepareStatement(sql);
			Object params[] = null;
			int k = 1;
			boolean flag = false;
			for (int i = 0; i < paramsList.size(); i++) {
				params = (Object[]) (Object[]) paramsList.get(i);
				for (int j = 0; j < params.length; j++)
					if (params[j] != null)
						ps.setObject(j + 1, params[j]);
					else
						ps.setNull(j + 1, 12);

				ps.addBatch();
				if (i == k * 8000)
					flag = true;
				if (i > k * 8000 && flag) {
					ps.executeBatch();
					k++;
					flag = false;
					ps.clearBatch();
				}
			}

			if (!flag)
				ps.executeBatch();
		} catch (SQLException exc) {
			throw new BOSException((new StringBuilder()).append(
					"Sql222 execute exception : ").append(sql).toString(), exc);
		} catch (ClassNotFoundException e) {
			throw new BOSException(
					"Sql222 execute exception : ClassNotFoundException", e);
		} finally {
			SQLUtils.cleanup(ps, conn);
		}

	}

	protected void _writeBack(Context ctx, String individualCodeStr,
			boolean isInware, boolean isPurInWarehs) throws BOSException,
			EASBizException {
		try {
			String updateSql = (new StringBuilder()).append(
					" update T_HCC_MaterialImforEntry set FIsPurInware = ")
					.append(!isPurInWarehs ? 0 : 1).append(",FIsInware = ")
					.append(!isInware ? 0 : 1).append(
							" where 1=1 and FIndividualCode in (").append(
							individualCodeStr).append(")").toString();
			DbUtil.execute(ctx, updateSql);
			updateSql = (new StringBuilder()).append(" update ").append(
					tableName).append(" set ISINWARE = ").append(
					!isInware ? 0 : 1).append(
					" where 1=1 and DQBZ = 1 and INDIVIDUALCODE in (").append(
					individualCodeStr).append(")").toString();
			executeSqlByDirectConnect(updateSql);
		} catch (BOSException e) {
			e.printStackTrace();
		}
	}

	private static final long serialVersionUID = 5586459710342454219L;
	private static Logger logger = Logger
			.getLogger("com.kingdee.eas.scm.hcc.common.app.AutoHighConsumeInfoFacadeControllerBean");
	private static String tableName = "T_DEP_HCC_MATERIALINFORMATION";
	private Map bacodeInventoryMap;
	private Map purReceivalMap;
	private Map adminMaterialMap;
	private Map materialPurchaseMap;
	private CoreBillBaseCollection srcChargeBillCollection;
	private CoreBillBaseCollection srcNoChargeBillCollection;
	private List idList;
	private List codeList;
	private List isInWareCodeList;
	private final String paramName = "AutoMaterialReqBill";
	private Map cacheParam;
	private Map messageCode2Str;
	public static String CONFIG_EXCEPTION = "CONFIG_EXCEPTION";
	private static final int MAX_DATA_ROW = 8000;

	/***
	 * 是否在运行，同一时刻，仅允许一个计费程序在运行
	 */
	private static boolean m_bRun = false;
	static {
		if (PorpertiesHelper.getIsDirectConnect().equals("1"))
			tableName = PorpertiesHelper.getMATERIAL_T_NAME();
	}
}
