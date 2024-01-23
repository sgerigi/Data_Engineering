--01. mv_tgt_ew_isr_fct_sales_stg
create materialized view fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg
as
with
benchmrk as ---rolling 12 months average revenue
  (select
      storecode
      ,case when avg_last_12_mnts between 0 and 500000 then '0-5L'
           when avg_last_12_mnts between 500000 and 1000000 then '5-10L'
           when avg_last_12_mnts between 1000000 and 2500000 then '10-25L'
           when avg_last_12_mnts between 2500000 and 5000000 then '25-50L'
           when avg_last_12_mnts >5000000 then '50L+'
       end as benchmark_group
     from
      (select
          storecode
          ,sum(value) as agg_value
          ,count(sale_year||sale_mon) as no_of_months
          ,sum(value)/count(sale_year||sale_mon) as avg_last_12_mnts
         from
          (select
              nvl(b.current_storecode,a.loc_code) as storecode
              ,extract(year from date(inv_dat)) as sale_year
              ,extract(month from date(inv_dat)) as sale_mon
              ,sum(ucp_val+NVL(other_chrg,0)-NVL(discount,0)) as value
             from ds_fld_digit_inboud_db.etl_wrk_data_bi.stg_isr_interim_bi a
               left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
                on a.loc_code = b.old_storecode
              where date(inv_dat)>=dateadd('month',-12,date_trunc('month',sysdate)) and date(inv_dat)<date_trunc('month',sysdate)
                and nvl(upper(bill_type),'') not in (select distinct upper(bill_type) from fielddigital_eyewear.t_tgt_ew_isr_dim_billtype_exclude)
              group by storecode,sale_year,sale_mon)
          group by storecode)),
store_dates as ---bring calender dates
  (select
      storecode
      ,bill_date
      ,abm_name
      ,abm_mailid
      ,ltl
      ,status
      ,storecity
      ,storeregion
      ,country
      ,channel
      ,divisionname
      ,open_date
      ,combo_status
      ,mall_status
      ,owner_type
      ,rbm_name
      ,rbm_mailid
     from
      (select
          storecode
          ,abm_name
          ,abm_mailid
          ,ltl
          ,status
          ,storecity
          ,storeregion
          ,country
          ,channel
          ,divisionname
          ,open_date
          ,combo_status
          ,mall_status
          ,owner_type
          ,rbm_name
          ,rbm_mailid
         from fielddigital_eyewear.t_tgt_fld_ew_dim_store_master
          where rec_status_cd=1 and storecode not in (select distinct old_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping 
														  where rec_status_cd=1 and current_storecode <> old_storecode)) stores
      cross join
       (select cal_date as bill_date from common.t_tgt_all_all_dim_calendar
            where cal_date between dateadd('year',-2,dateadd(month,3,date_trunc('year',dateadd(month,-3,sysdate))))
              and (select max(date(inv_dat)) from ds_fld_digit_inboud_db.etl_wrk_data_bi.stg_isr_interim_bi
                      where date(inv_dat)<=sysdate)) cal),
fct as
 (select
    nvl(b.current_storecode,a.loc_code) as storecode ---convert old storecode to new store code
    ,date(inv_dat) as bill_date
    ,inv_num
    ,txn_ln_num
    ,posidex_unified_id as ucic
    ,order_typ
    ,bill_type
    ,cust_code
    ,txn_typ_cd
    ,itm_cd
    ,rtl_qty
    ,sls_order_num
    ,pos_inv_num
    ,ucp_val
    ,(ucp_val+NVL(other_chrg,0)) as gucp_val
    ,(ucp_val+NVL(other_chrg,0)-NVL(discount,0)) as nucp_val
    ,sale_per
    ,mer_cat
    ,other_chrg
    ,other_charges
   from ds_fld_digit_inboud_db.etl_wrk_data_bi.stg_isr_interim_bi a
     left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
      on a.loc_code = b.old_storecode
     where date(inv_dat)>=dateadd('year',-2,dateadd(month,3,date_trunc('year',dateadd(month,-3,sysdate))))
       and nvl(upper(bill_type),'') not in (select distinct upper(bill_type) from fielddigital_eyewear.t_tgt_ew_isr_dim_billtype_exclude)),
item_sales as
  (select
     storecode
     ,bill_date
     ,abm_name
     ,abm_mailid
     ,ltl
     ,status
     ,storecity
     ,storeregion
     ,country
     ,channel
     ,divisionname
     ,open_date
     ,combo_status
     ,mall_status
     ,owner_type
     ,rbm_name
     ,rbm_mailid
     ,inv_num
     ,txn_ln_num
     ,ucic
     ,order_typ
     ,bill_type
     ,cust_code
     ,txn_typ_cd
     ,itm_cd
     ,rtl_qty
     ,sls_order_num
     ,pos_inv_num
     ,ucp_val
     ,gucp_val
     ,nucp_val
     ,sale_per
     ,mer_cat
     ,other_chrg
     ,other_charges
     ,othr_lens_sku_code
     ,pf_lens_sku_code
     ,lens_category
     ,case when upper(lens_category)='RX SG' or other_chrg>0 then 'RX SG' --RX SG condition for lens
      end as lens_tag
     ,brnd_cd
     ,brnd_nm
     ,brnd_grp_cd
     ,brnd_grp_nm
     ,cat_nm
    from
  (select
     storecode
     ,bill_date
     ,abm_name
     ,abm_mailid
     ,ltl
     ,status
     ,storecity
     ,storeregion
     ,country
     ,channel
     ,divisionname
     ,open_date
     ,combo_status
     ,mall_status
     ,owner_type
     ,rbm_name
     ,rbm_mailid
     ,inv_num
     ,txn_ln_num
     ,ucic
     ,order_typ
     ,bill_type
     ,cust_code
     ,txn_typ_cd
     ,fsale.itm_cd
     ,rtl_qty
     ,sls_order_num
     ,pos_inv_num
     ,ucp_val
     ,gucp_val
     ,nucp_val
     ,sale_per
     ,mer_cat
     ,other_chrg
     ,other_charges
     ,othr_lens_sku_code
     ,pf_lens_sku_code
     ,case when upper(item.cat_nm) like '%LENS%' and upper(item.cat_nm)<>'CONTACT LENSES' then
             case when fsale.itm_cd = othr_lens_sku_code then othr_lens_category --other than Progres & fsv
                  when fsale.lens_pf_itm_cd = pf_lens_sku_code then pf_lens_category --Progres & fsv
                  else 'NOT MAPPED'
             end
      end as lens_category
     ,item.brnd_cd
     ,nvl(item.brnd_nm,'NOT MAPPED') as brnd_nm
     ,nvl(item.brnd_grp_cd,'NOT MAPPED') as brnd_grp_cd
     ,nvl(item.brnd_grp_nm,'NOT MAPPED') as brnd_grp_nm
     ,nvl(item.cat_nm,'NOT MAPPED') as cat_nm
    from
     (select
        store_dates.storecode
        ,store_dates.bill_date
        ,abm_name
        ,abm_mailid
        ,ltl
        ,status
        ,storecity
        ,storeregion
        ,country
        ,channel
        ,divisionname
        ,open_date
        ,combo_status
        ,mall_status
        ,owner_type
        ,rbm_name
        ,rbm_mailid
        ,inv_num
        ,txn_ln_num
        ,nvl(ucic,0) as ucic
        ,order_typ
        ,bill_type
        ,cust_code
        ,txn_typ_cd
        ,itm_cd
        ,substring(itm_cd,0,8) as lens_pf_itm_cd --- for progressive and fsv lens, fetch 1st 7 char to join with lens category
        ,nvl(rtl_qty,0) as rtl_qty
        ,sls_order_num
        ,pos_inv_num
        ,nvl(ucp_val,0) as ucp_val
        ,nvl(gucp_val,0) as gucp_val
        ,nvl(nucp_val,0) as nucp_val
        ,sale_per
        ,mer_cat
        ,other_chrg
        ,other_charges
       from store_dates
         left join fct
          on store_dates.storecode = fct.storecode
          and store_dates.bill_date = fct.bill_date) fsale
      left join (select itm_cd,brnd_cd,brnd_nm,brnd_grp_cd,brnd_grp_nm,cat_nm from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_stg_ew_isr_dim_itemmaster where rec_status_cd=1) item
        on fsale.itm_cd=item.itm_cd
      left join (select base_sku_code as othr_lens_sku_code,lens_category as othr_lens_category from fielddigital_eyewear.t_tgt_fld_ew_dim_lens_category
            where rec_status_cd=1 and upper(lens_category) not in ('PROGRESSIVE','FSV')) othr_lens_cat
        on fsale.itm_cd = othr_lens_cat.othr_lens_sku_code
      left join (select base_sku_code as pf_lens_sku_code,lens_category as pf_lens_category from fielddigital_eyewear.t_tgt_fld_ew_dim_lens_category
              where rec_status_cd=1 and upper(lens_category) in ('PROGRESSIVE','FSV')) pf_lens_cat
        on fsale.lens_pf_itm_cd = pf_lens_cat.pf_lens_sku_code)),
rx_sg_lens AS --rx lens count for each invoice to assign customer frames as sunglass
   (select
      inv_num
      ,count(txn_ln_num) as rx_sg_ln_count
     from item_sales
      where lower(cat_nm) like '%lens%' and lower(cat_nm)<>'contact lenses'
         and lens_tag='RX SG'
     group by inv_num),
fr_sg_rn as --assign ranks for frames & sunglass
 (select
     storecode
     ,bill_date
     ,inv_num
     ,txn_ln_num
     ,drv_cat
     ,row_number() over(partition by inv_num order by lower(drv_cat),txn_ln_num) as fr_sg_rn
     ,row_number() over(partition by inv_num order by lower(drv_cat) desc,txn_ln_num) as sg_fr_rn
    from
     (select
       storecode
       ,bill_date
       ,item_sales.inv_num
       ,txn_ln_num
       ,case when upper(mer_cat)<>'CUS-FRAME' and lower(cat_nm) like '%frames%' then 'FRAME'
             when upper(mer_cat)<>'CUS-FRAME' and lower(cat_nm) like '%sunglass%' then 'SUNGLASS'
             when upper(mer_cat)='CUS-FRAME' and rx_sg_ln_count>=2 then 'SUNGLASS' --for rx sunglass, cust-frame should also consider if proper rx lens are available
        end drv_cat
      from item_sales
        left join rx_sg_lens
          on item_sales.inv_num=rx_sg_lens.inv_num)
     where drv_cat in ('FRAME','SUNGLASS')),
lens_rn as --assign ranks for lens. RX lens (lens_tag) should be given priority in ranks
  (select
     storecode
     ,bill_date
     ,inv_num
     ,txn_ln_num
     ,cat_nm
     ,lens_tag
     ,row_number() over(partition by inv_num order by lens_tag,txn_ln_num) as ln_rn
    from item_sales
     where lower(cat_nm) like '%lens%' and lower(cat_nm)<>'contact lenses'
        and nvl(upper(mer_cat),'')<>'CUS-LENS'),
spec_cnt as
 (select
     inv_num
     ,case when fr_ct<fr_sg_mx then fr_ct
           else fr_sg_mx
      end as fr_ct --- defining max possible spects
     ,rx_ln_mx
     ,rx_ln_mx/2 as rx_sg_mx
     ,sg_mx
     from
   (select
       lens_rn.inv_num
       ,max(ln_rn) as ln_mx --max no of lens
       ,max(ln_rn)/2 as fr_ct -- max possible spects using lens pairs
       ,max(fr_sg_rn) as fr_sg_mx -- max possible spects using frames/sunglass
       ,max(case when lens_tag='RX SG' then ln_rn end) as rx_ln_mx --max no of rx sg lens
       ,count(distinct case when lower(fr_sg_rn.drv_cat) like '%sunglass%' then fr_sg_rn.txn_ln_num end) as sg_mx --max no of sunglasses
      from lens_rn
        inner join fr_sg_rn
          on lens_rn.inv_num = fr_sg_rn.inv_num
      group by lens_rn.inv_num)),
sales as
( select
    storecode
   ,bill_date
   ,abm_name
   ,abm_mailid
   ,ltl
   ,status
   ,storecity
   ,storeregion
   ,country
   ,channel
   ,divisionname
   ,open_date
   ,combo_status
   ,mall_status
   ,owner_type
   ,rbm_name
   ,rbm_mailid
   ,inv_num
   ,txn_ln_num
   ,ucic
   ,order_typ
   ,bill_type
   ,cust_code
   ,txn_typ_cd
   ,itm_cd
   ,rtl_qty
   ,sls_order_num
   ,pos_inv_num
   ,ucp_val
   ,gucp_val
   ,nucp_val
   ,sale_per
   ,mer_cat
   ,other_chrg
   ,other_charges
   ,othr_lens_sku_code
   ,pf_lens_sku_code
   ,lens_category
   ,brnd_cd
   ,brnd_nm
   ,brnd_grp_cd
   ,brnd_grp_nm
   ,cat_nm
   ,lens_tag
   ,fin_fr_sg_rn
   ,fr_sg_rn
   ,sg_fr_rn
   ,rx_sg_mx
   ,ln_rn
   ,fr_ct
   ,case when fin_fr_sg_rn<=fr_ct and fin_fr_sg_rn<=rx_sg_mx then 'RX SG' -- assigning RX SG to frames/sunglass based on rx lens
         when fin_fr_sg_rn<=fr_ct and (fin_fr_sg_rn>rx_sg_mx or rx_sg_mx is null) then 'SR' --assigning SR to frames/sunglass based on rx lens
         when ln_rn<=2*fr_ct and lens_tag='RX SG' then 'RX SG' -- assigning RX SG to lens
         when ln_rn<=2*fr_ct and nvl(lens_tag,'')<>'RX SG' then 'SR' -- assigning SR to lens which are not RX lens
         else 'O' --assigning O to remaining items
    end as spec_tag
  from
(select
   item_sales.storecode
   ,item_sales.bill_date
   ,abm_name
   ,abm_mailid
   ,ltl
   ,status
   ,storecity
   ,storeregion
   ,country
   ,channel
   ,divisionname
   ,open_date
   ,combo_status
   ,mall_status
   ,owner_type
   ,rbm_name
   ,rbm_mailid
   ,item_sales.inv_num
   ,item_sales.txn_ln_num
   ,ucic
   ,order_typ
   ,bill_type
   ,cust_code
   ,txn_typ_cd
   ,itm_cd
   ,rtl_qty
   ,sls_order_num
   ,pos_inv_num
   ,ucp_val
   ,gucp_val
   ,nucp_val
   ,sale_per
   ,mer_cat
   ,other_chrg
   ,other_charges
   ,othr_lens_sku_code
   ,pf_lens_sku_code
   ,lens_category
   ,brnd_cd
   ,brnd_nm
   ,brnd_grp_cd
   ,brnd_grp_nm
   ,item_sales.cat_nm
   ,fr_sg_rn.fr_sg_rn
   ,fr_sg_rn.sg_fr_rn
   ,item_sales.lens_tag
   ,lens_rn.ln_rn
   ,spec_cnt.fr_ct
   ,spec_cnt.rx_ln_mx
   ,spec_cnt.rx_sg_mx
   ,spec_cnt.sg_mx
   ,case when sg_mx>0 and rx_sg_mx>0 then
            case when sg_fr_rn <= rx_sg_mx then sg_fr_rn
                 when sg_fr_rn > rx_sg_mx and drv_cat='SUNGLASS' then fr_sg_rn
                 else fr_sg_rn+rx_sg_mx
            end
         else fr_sg_rn
    end as fin_fr_sg_rn ---changing ranking for sunglass and frames based on no of rx lens
  from item_sales
    left join fr_sg_rn
      on item_sales.inv_num = fr_sg_rn.inv_num
      and item_sales.txn_ln_num =fr_sg_rn.txn_ln_num
    left join lens_rn
      on item_sales.inv_num = lens_rn.inv_num
      and item_sales.txn_ln_num =lens_rn.txn_ln_num
    left join spec_cnt
      on item_sales.inv_num=spec_cnt.inv_num)),
selling_score as
  (select
     storecode
     ,order_date
     ,order_number
     ,flag
     ,case when upper(flag)='PINK' then 1
           else 0
      end as opp_corr_upsell_score
     ,case when upper(flag)='YELLOW' then 1
           else 0
      end as acc_upsell_score
     ,case when upper(flag)='GREEN' then 1
           else 0
      end as hon_sell_score
     ,case when upper(flag)='RED' then 1
           else 0
      end as pot_dis_hon_sell_score
     ,case when upper(flag)='UNIDENTIFIED' then 1
           else 0
      end as unid_score
    from fielddigital_eyewear.t_tgt_fld_ew_dim_ord_selling_score
     where rec_status_cd=1),
rx_test as
  (select
     nvl(b.current_storecode,a.storecode) as storecode
     ,vbeln
     ,pr_examin_by
    from
     (select
        storecode
        ,upper(trim(vbeln)) as vbeln
        ,erdat
        ,pr_examin_by
        ,row_number() over(partition by storecode,upper(trim(vbeln)) order by erdat desc) as rn
       from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_ew_isr_trns_soheader_bi
        where lower(pr_examin_by) in ('outside rx','inhouse rx')) a
      left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
       on a.storecode = b.old_storecode
     where rn=1),
ordr as
  (select
    sale_order_no
    ,order_date
   from
    (select upper(trim(sale_order_no)) as sale_order_no
      ,date(createddate) as order_date
      ,row_number() over(partition by upper(trim(sale_order_no)) order by date(createddate)) as rn
     from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_stg_ew_isr_trns_saleorder_bi)
    where rn=1)
select
   sales.storecode
   ,bill_date
   ,inv_num
   ,txn_ln_num
   ,ucic
   ,order_typ
   ,bill_type
   ,nvl(rx_test.pr_examin_by,'NOT MAPPED') as pr_examin_by
   --,nvl(lens_cat.lens_category,'NOT MAPPED') as lens_category
   ,itm_cd
   ,othr_lens_sku_code
   ,pf_lens_sku_code
   ,lens_category
   ,brnd_nm
   ,brnd_grp_cd
   ,brnd_grp_nm
   ,cat_nm
   ,lens_tag
   ,ordr.order_date
   ,cust_code
   ,txn_typ_cd
   ,rtl_qty
   ,upper(trim(sls_order_num)) as sls_order_num
   ,pos_inv_num
   ,ucp_val
   ,gucp_val
   ,nucp_val
   ,mer_cat
   ,other_chrg
   ,other_charges
   ,fin_fr_sg_rn
   ,ln_rn
   ,fr_ct
   ,rx_sg_mx
   ,case when spec_tag='O' and upper(mer_cat)='FBSMJRX' then 'SMJ RX SG' ---items which consider as RX SG
         else spec_tag
    end as spec_tag
   ,case when upper(sale_per) like ('%STORE%') or upper(sale_per) like ('%CASH%') or sale_per = '' or sale_per is null then 'SALE_PER NOT MAPPED'
         else trim(sale_per)
    end as sale_per
   ,nvl(opp_corr_upsell_score,0) as opp_corr_upsell_score
   ,nvl(acc_upsell_score,0) as acc_upsell_score
   ,nvl(unid_score,0) as unid_score
   ,nvl(hon_sell_score,0) as hon_sell_score
   ,nvl(pot_dis_hon_sell_score,0) as pot_dis_hon_sell_score
   ,abm_name
   ,abm_mailid
   ,ltl
   ,status
   ,storecity
   ,storeregion
   ,country
   ,channel
   ,divisionname
   ,open_date
   ,combo_status
   ,mall_status
   ,owner_type
   ,rbm_name
   ,rbm_mailid
   ,round(months_between(sysdate,open_date),2) as store_age
   ,nvl(benchmrk.benchmark_group,'0-5L') as benchmark_group
  from
    sales
    left join rx_test
      on sales.sls_order_num = rx_test.vbeln
      and sales.storecode = rx_test.storecode
    left join ordr
      on sales.sls_order_num = ordr.sale_order_no
    left join selling_score
      on sales.sls_order_num = selling_score.order_number
    left join benchmrk
      on sales.storecode = benchmrk.storecode;

--02. mv_ew_store_tar_achvmnt
create materialized view fielddigital_eyewear.mv_ew_store_tar_achvmnt
as
with
fct as
  (select
      storecode
      ,bill_date
      ,inv_num
      ,txn_ln_num
      ,ucic
      ,order_typ
      ,bill_type
      ,pr_examin_by
      ,brnd_nm
      ,brnd_grp_cd
      ,brnd_grp_nm
      ,cat_nm
      ,lens_category
      ,cust_code
      ,txn_typ_cd
      ,itm_cd
      ,rtl_qty
      ,sls_order_num
      ,upper(trim(pos_inv_num)) as pos_inv_num
      ,ucp_val
      ,gucp_val
      ,nucp_val
      ,sale_per
      ,mer_cat
      ,spec_tag
      ,order_date
      ,opp_corr_upsell_score
      ,acc_upsell_score
      ,unid_score
      ,hon_sell_score
      ,pot_dis_hon_sell_score
      ,abm_name
      ,abm_mailid
      ,ltl
      ,status
      ,storecity
      ,storeregion
      ,country
      ,channel
      ,divisionname
      ,open_date
      ,store_age
      ,benchmark_group
      ,combo_status
      ,mall_status
      ,owner_type
      ,rbm_name
      ,rbm_mailid
    from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg),
---- walkins query: distinct is used to removes duplicated as walkins data multuple records on single day. fetching daywise walkins at storelevel
walkins as
  (select
      storecode
     ,shoperdate
     ,sum(groupcount) as walkin_cnt
     from
      (select
         distinct
         nvl(b.current_storecode,a.site) as storecode
         ,shoperdate
         ,groupcount
        from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_stg_ew_isr_trns_walkin_bi a
         left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
          on a.site = b.old_storecode)
     group by storecode,shoperdate),
bills_buyers as
  (select
      storecode
      ,bill_date
      ,count(distinct (case when upper(txn_typ_cd) like 'M%' then storecode||bill_date||inv_num end))+
       count(distinct (case when upper(txn_typ_cd) like 'S%' then storecode||bill_date||inv_num end))-
       count(distinct (case when upper(txn_typ_cd) like 'N%' then storecode||bill_date||inv_num end))-
       count(distinct (case when upper(txn_typ_cd) like 'O%' then storecode||bill_date||inv_num end)) as no_of_bills
      ,sum(nucp_val) as value
      ,abm_name
      ,abm_mailid
      ,ltl
      ,status
      ,storecity
      ,storeregion
      ,country
      ,channel
      ,divisionname
      ,open_date
      ,store_age
      ,benchmark_group
      ,combo_status
      ,mall_status
      ,owner_type
      ,rbm_name
      ,rbm_mailid
     from fct
      group by storecode,bill_date,abm_name,abm_mailid,ltl,status,storecity,storeregion,country,channel,divisionname,open_date,store_age,benchmark_group,combo_status,mall_status,owner_type,rbm_name,rbm_mailid),
fct_buyers as
 (select distinct
    storecode
    ,bill_date
    ,extract(year from bill_date) as bill_year
    ,extract(month from bill_date) as bill_month
    ,DATE_PART(year,(DATEADD(month,-3,bill_date))) as fin_year
    ,cust_code
   from fct),
mtd_buyers as
  (select
     storecode
     ,bill_year
     ,bill_month
     ,count(distinct cust_code) as no_of_buyers_mtd
    from fct_buyers
      group by storecode,bill_year,bill_month),
ytd_buyers as
  (select
     storecode
     ,fin_year
     ,count(distinct cust_code) as no_of_buyers_ytd
    from fct_buyers
      group by storecode,fin_year),
bds_sales as
  (select
      storecode
      ,order_date
      ,sum(netprice) as bds_value
      ,count(distinct storecode||order_date||customer_number) as no_of_bds_buyers
     from
       (select
           nvl(b.current_storecode,a.store_code) as storecode
           ,sale_order_no
           ,date(createddate) as order_date
           ,customer_number
           ,netprice
           ,sddocumenttype
           ,reason_for_rejections
          from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_stg_ew_isr_trns_saleorder_bi a
            left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
                on a.store_code = b.old_storecode
           where upper(itemcategorycode)='TAN'
             and isnull(reason_for_rejections,'') in ('0','')) a
         left join ds_fld_digit_inboud_db.edw_base_data_bi.t_tgt_ew_isr_mstr_tvakt_salesdoctypemaster_bi b
           on a.sddocumenttype =b.auart
        where upper(b.SALETYPE) in ('DIRECT SALES','BOOKING SALES')
        group by storecode,order_date),
bds_buyers as
 (select a.*
    from
     (select
         nvl(b.current_storecode,a.store_code) as storecode
         ,sale_order_no
         ,date(createddate) as order_date
         ,extract(year from date(createddate)) as bill_year
         ,extract(month from date(createddate)) as bill_month
         ,DATE_PART(year,(DATEADD(month,-3,date(createddate)))) as fin_year
         ,customer_number
         ,netprice
         ,sddocumenttype
         ,reason_for_rejections
        from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_stg_ew_isr_trns_saleorder_bi a
            left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
              on a.store_code = b.old_storecode
         where upper(itemcategorycode)='TAN'
           and isnull(reason_for_rejections,'') in ('0','')) a
       left join ds_fld_digit_inboud_db.edw_base_data_bi.t_tgt_ew_isr_mstr_tvakt_salesdoctypemaster_bi b
         on a.sddocumenttype =b.auart
      where upper(b.SALETYPE) in ('DIRECT SALES','BOOKING SALES')),
mtd_bds_buyers as
  (select
     storecode
     ,bill_year
     ,bill_month
     ,count(distinct customer_number) as no_of_bds_buyers_mtd
    from bds_buyers
      group by storecode,bill_year,bill_month),
ytd_bds_buyers as
  (select
     storecode
     ,fin_year
     ,count(distinct customer_number) as no_of_bds_buyers_ytd
    from bds_buyers
      group by storecode,fin_year),
rx_test as
(select
  a.storecode
  ,a.order_date as bill_date
  ,sum(case when lower(pr_examin_by)='outside rx' then netprice end) as outside_rx_value
  ,sum(netprice) as alltype_examin_value
  ,count(distinct case when lower(pr_examin_by)='outside rx' then sale_order_no end) as out_rx_txns
  ,count(distinct sale_order_no) as txns
from
  (select
     storecode
     ,vbeln
     ,pr_examin_by
     ,order_date
   from
     (select
        storecode
        ,upper(trim(vbeln)) as vbeln
        ,erdat as order_date
        ,pr_examin_by
        ,row_number() over(partition by storecode,upper(trim(vbeln)) order by erdat desc) as rn
       from (select
                nvl(b.current_storecode,a.storecode) as storecode
                ,vbeln
                ,erdat
                ,pr_examin_by
               from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_ew_isr_trns_soheader_bi a
                 left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
                   on a.storecode = b.old_storecode))
         where rn=1) a
     inner join
       (select
           nvl(b.current_storecode,a.store_code) as storecode
           ,upper(trim(sale_order_no)) as sale_order_no
           ,date(createddate) as order_date
           ,customer_number
           ,netprice
           ,sddocumenttype
           ,reason_for_rejections
          from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_stg_ew_isr_trns_saleorder_bi a
           left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
             on a.store_code = b.old_storecode) b
           on  a.vbeln = b.sale_order_no
           and a.storecode = b.storecode
   group by a.storecode,a.order_date),
rx_sung as
  (select
      storecode
      ,bill_date
      ,sum(case when upper(spec_tag)='SMJ RX SG' then rtl_qty else 0 end)+
       (sum(case when upper(spec_tag)='RX SG' then rtl_qty else 0 end)/3) as rx_sung_vol
     from fct
      where upper(spec_tag) in ('RX SG','SMJ RX SG')
      group by storecode,bill_date),
spec_ticket as
  (select
     storecode
     ,bill_date
     ,count(distinct (case when upper(txn_typ_cd) like 'M%' then storecode||bill_date||inv_num end))+
      count(distinct (case when upper(txn_typ_cd) like 'S%' then storecode||bill_date||inv_num end))-
      count(distinct (case when upper(txn_typ_cd) like 'N%' then storecode||bill_date||inv_num end))-
      count(distinct (case when upper(txn_typ_cd) like 'O%' then storecode||bill_date||inv_num end)) as spec_no_of_bills
     ,sum(nucp_val) as spec_value
    from fct
     where upper(spec_tag)='SR'
     group by storecode,bill_date),
sunglass as
  (select
      storecode
      ,bill_date
      ,sum(rtl_qty) as sung_volume
      ,sum(nucp_val) as sung_value
     from fct
       where lower(cat_nm) like '%sunglass%' and nvl(upper(mer_cat),'')<>'CUS-FRAME'
      group by storecode,bill_date),
sale_vol as
  (select
      storecode
      ,bill_date
      ,sum(volume) as volume
     from
      (select
         cust_code
         ,bill_date
         ,storecode
         ,isnull(sum(distinct case when upper(spec_tag) in ('SR','RX SG') then 1 end),0)+
          isnull(sum(case when upper(spec_tag) not in ('SR','RX SG') and lower(cat_nm) not like '%lens%' then rtl_qty end),0)+
          isnull(sum(case when upper(spec_tag) not in ('SR','RX SG') and lower(cat_nm) like '%lens%' then 1 end),0) as volume
        from fct
         group by cust_code,bill_date,storecode)
      group by storecode,bill_date),
selling_scores as
  (select
      storecode
      ,bill_date
      ,sum(opp_corr_upsell_score) as opp_corr_upsell_score
      ,sum(acc_upsell_score) as acc_upsell_score
      ,sum(unid_score) as unid_score
      ,sum(hon_sell_score) as hon_sell_score
      ,sum(pot_dis_hon_sell_score) as pot_dis_hon_sell_score
     from
      (select distinct
          storecode
          ,bill_date
          ,sls_order_num
          ,opp_corr_upsell_score
          ,acc_upsell_score
          ,unid_score
          ,hon_sell_score
          ,pot_dis_hon_sell_score
         from fct
           where sls_order_num is not null)
      group by storecode,bill_date),
comfort_calling as
  (select
      storecode
      ,request_date as bill_date
      ,count(distinct phone_number||request_date) as tot_comf_call_surveys
      ,count(distinct case when lower(answer)='"yes (1)"' then phone_number||request_date end) as comf_call_yes_cnt
      ,count(distinct case when lower(answer)='"no (0)"' then phone_number||request_date end) as comf_call_no_cnt
    from
     (select distinct
        nvl(b.current_storecode,a.touchpoint_name) as storecode
        ,phone_number
        ,date(request_date) as request_date
        ,question
        ,answer
      from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_isr_litmus_fct_eyeplus_15_days_bi a
        left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
         on a.touchpoint_name = b.old_storecode
       where date(responded_on)>='2022-04-01'
         and lower(question) ='"did you recieve a call from the store team to check your product comfort ?"')
     group by storecode,request_date),
eyetest_exp as
 (select
    storecode
    ,request_date as bill_date
    ,count(distinct phone_number||request_date) as tot_eyetest_responses
    ,count(distinct case when lower(answer) in ('" (9)"','"extremely satisfied (10)"') then phone_number||request_date end) as eyetest_promot_cnt
    ,count(distinct case when lower(answer) in ('" (7)"','" (8)"') then phone_number||request_date end) as eyetest_passiv_cnt
    ,count(distinct case when lower(answer) in ('" (1)"','" (2)"','" (3)"','" (4)"','" (5)"','" (6)"','"1 (1)"','"2 (2)"','"3 (3)"','"4 (4)"','"5 (5)"','"extremely unsatisfied (0)"') then phone_number||request_date end) as eyetest_detract_cnt
   from
    (select distinct
         nvl(b.current_storecode,a.touchpoint_name) as storecode
         ,phone_number
         ,date(request_date) as request_date
         ,question
         ,answer
       from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_isr_litmus_fct_eyeplus_2_days_bi a
        left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
         on a.touchpoint_name = b.old_storecode
        where date(responded_on)>='2022-04-01'
          and lower(question) ='"eye-testing experience"')
    group by storecode,request_date),
prodct_variety as
  (select
     storecode
     ,request_date as bill_date
     ,count(distinct phone_number||request_date) as tot_prd_vrty_responses
     ,count(distinct case when lower(answer) in ('" (9)"','"extremely satisfied (10)"') then phone_number||request_date end) as prd_vrty_promot_cnt
     ,count(distinct case when lower(answer) in ('" (7)"','" (8)"') then phone_number||request_date end) as prd_vrty_passiv_cnt
     ,count(distinct case when lower(answer) in ('" (1)"','" (2)"','" (3)"','" (4)"','" (5)"','" (6)"','"1 (1)"','"2 (2)"','"3 (3)"','"4 (4)"','"5 (5)"','"extremely unsatisfied (0)"') then phone_number||request_date end) as prd_vrty_detract_cnt
    from
     (select distinct
        nvl(b.current_storecode,a.touchpoint_name) as storecode
        ,phone_number
        ,date(request_date) as request_date
        ,question
        ,answer
      from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_isr_litmus_fct_eyeplus_2_days_bi a
        left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
         on a.touchpoint_name = b.old_storecode
       where date(responded_on)>='2022-04-01'
         and lower(question) ='"product variety"')
     group by storecode,request_date),
ease_billing as
  (select
     storecode
     ,request_date as bill_date
     ,count(distinct phone_number||request_date) as tot_ease_billing_responses
     ,count(distinct case when lower(answer) in ('" (9)"','"extremely satisfied (10)"') then phone_number||request_date end) as ease_billing_promot_cnt
     ,count(distinct case when lower(answer) in ('" (7)"','" (8)"') then phone_number||request_date end) as ease_billing_passiv_cnt
     ,count(distinct case when lower(answer) in ('" (1)"','" (2)"','" (3)"','" (4)"','" (5)"','" (6)"','"1 (1)"','"2 (2)"','"3 (3)"','"4 (4)"','"5 (5)"','"extremely unsatisfied (0)"') then phone_number||request_date end) as ease_billing_detract_cnt
    from
     (select distinct
         nvl(b.current_storecode,a.touchpoint_name) as storecode
         ,phone_number
         ,date(request_date) as request_date
         ,question
         ,answer
       from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_isr_litmus_fct_eyeplus_2_days_bi a
        left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
         on a.touchpoint_name = b.old_storecode
        where date(responded_on)>='2022-04-01'
          and lower(question) ='"ease of billing"')
     group by storecode,request_date),
on_time_delivr as
  (select
     storecode
     ,request_date as bill_date
     ,count(distinct phone_number||request_date) as tot_on_time_delivr_responses
     ,count(distinct case when lower(answer) in ('" (9)"','"extremely satisfied (10)"') then phone_number||request_date end) as on_time_delivr_promot_cnt
     ,count(distinct case when lower(answer) in ('" (7)"','" (8)"') then phone_number||request_date end) as on_time_delivr_passiv_cnt
     ,count(distinct case when lower(answer) in ('" (1)"','" (2)"','" (3)"','" (4)"','" (5)"','" (6)"','"1 (1)"','"2 (2)"','"3 (3)"','"4 (4)"','"5 (5)"','"extremely unsatisfied (0)"') then phone_number||request_date end) as on_time_delivr_detract_cnt
    from
     (select distinct
         nvl(b.current_storecode,a.touchpoint_name) as storecode
         ,phone_number
         ,date(request_date) as request_date
         ,question
         ,answer
       from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_isr_litmus_fct_eyeplus_2_days_bi a
        left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
         on a.touchpoint_name = b.old_storecode
        where date(responded_on)>='2022-04-01'
          and lower(question) ='"on time delivery of your product"')
     group by storecode,request_date),
litmus_2days AS
  (select
     storecode
     ,request_date
     ,count(distinct phone_number||request_date) as no_of_responses_2days
     ,count(distinct case when rating between 9 and 10 then phone_number||request_date end) as promoters_cnt_2days
     ,count(distinct case when rating between 7 and 8 then phone_number||request_date end) as passives_cnt_2days
     ,count(distinct case when rating<=6 then phone_number||request_date end) as detractors_cnt_2days
    from
     (select
        nvl(b.current_storecode,a.touchpoint_name) as storecode
        ,phone_number
        ,date(request_date) as request_date
        ,max(cast(primary_question as int)) as rating
       from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_isr_litmus_fct_eyeplus_2_days_bi a
        left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
         on a.touchpoint_name = b.old_storecode
        where date(responded_on)>='2022-04-01'
        group by touchpoint_name,current_storecode,phone_number,date(request_date))
    group by storecode,request_date),
litmus_15days as
  (select
     storecode
     ,request_date
     ,count(distinct phone_number||request_date) as no_of_responses_15days
     ,count(distinct case when rating between 9 and 10 then phone_number||request_date end) as promoters_cnt_15days
     ,count(distinct case when rating between 7 and 8 then phone_number||request_date end) as passives_cnt_15days
     ,count(distinct case when rating<=6 then phone_number||request_date end) as detractors_cnt_15days
    from
     (select
        nvl(b.current_storecode,a.touchpoint_name) as storecode
        ,phone_number
        ,date(request_date) as request_date
        ,max(cast(primary_question as numeric(10,1))) as rating
       from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_isr_litmus_fct_eyeplus_15_days_bi a
        left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
         on a.touchpoint_name = b.old_storecode
        where date(responded_on)>='2022-04-01'
        group by touchpoint_name,current_storecode,phone_number,date(request_date))
     group by storecode,request_date),
litmus_90days as
  (select
     storecode
     ,request_date
     ,count(distinct phone_number||request_date) as no_of_responses_90days
     ,count(distinct case when rating between 9 and 10 then phone_number||request_date end) as promoters_cnt_90days
     ,count(distinct case when rating between 7 and 8 then phone_number||request_date end) as passives_cnt_90days
     ,count(distinct case when rating<=6 then phone_number||request_date end) as detractors_cnt_90days
    from
     (select
        nvl(b.current_storecode,a.touchpoint_name) as storecode
        ,phone_number
        ,date(request_date) as request_date
        ,max(cast(primary_question as int)) as rating
       from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_isr_litmus_fct_eyeplus_90_days_bi a
        left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
         on a.touchpoint_name = b.old_storecode
        where date(responded_on)>='2022-04-01'
        group by touchpoint_name,current_storecode,phone_number,date(request_date))
     group by storecode,request_date),
litmus_330days as
  (select
     storecode
     ,request_date
     ,count(distinct phone_number||request_date) as no_of_responses_330days
     ,count(distinct case when rating between 9 and 10 then phone_number||request_date end) as promoters_cnt_330days
     ,count(distinct case when rating between 7 and 8 then phone_number||request_date end) as passives_cnt_330days
     ,count(distinct case when rating<=6 then phone_number||request_date end) as detractors_cnt_330days
    from
     (select
        nvl(b.current_storecode,a.touchpoint_name) as storecode
        ,phone_number
        ,date(request_date) as request_date
        ,max(cast(primary_question as int)) as rating
       from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_ew_litmus_trns_eyeplus_330_bi a
        left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
         on a.touchpoint_name = b.old_storecode
        where date(responded_on)>='2022-04-01'
        group by touchpoint_name,current_storecode,phone_number,date(request_date))
     group by storecode,request_date),
nps_surveys as
   (select
      storecode
      ,bill_date
      ,count(distinct cust_code) as no_of_nps_surveys
     from fct
       where upper(txn_typ_cd) like 'M%'
      group by storecode,bill_date),
---- fetching GMB reviews at day level with latest 2 finance years data
gmb_reviews as
  (select
     storecode
     ,review_date
     ,count(review_id) as no_of_reviews
     ,sum(rating) as agg_rating
    from
   (select
       nvl(b.current_storecode,a.storecode) as storecode
       ,update_date as review_date
       ,review_id
       ,rating
      from fielddigital_eyewear.t_tgt_fld_ew_dim_gmb_review a
        left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
         on a.storecode = b.old_storecode
       where review_date>=dateadd('year',-1,dateadd(month,3,date_trunc('year',dateadd(month,-3,sysdate))))
          and rec_status_cd = 1)
      group by storecode,review_date),
---- excluding shopper stop customers as suggested and derivig no of customers with 15 days condition for calculating AUPC
no_of_customers AS
(select
   storecode
   ,bill_date
   ,count(cust_code) as no_of_customers
  from
   (select
       storecode
       ,bill_date
       ,cust_code
       ,case when bill_date - NVL(lag(bill_date) over (partition by storecode,cust_code order by bill_date), bill_date)
                  between 1 and 15 then 'lt15'
             else 'gt15'
        end as day_tag
      from
        (select distinct
            storecode
            ,bill_date
            ,cust_code
           from fct
             left join
                (select gld_map.ucic,firstname from ds_fld_digit_inboud_db.sem_pu_cust_all.t_tgt_cust_all_ucic_lylty_mapng_bi gld_map
                   inner join (select ulpmembershipid,firstname from
                    (select
                       ulpmembershipid
                       ,firstname
                       ,row_number() over(partition by ulpmembershipid order by firstname) as rn
                     from ds_fld_digit_inboud_db.edw_base_data_bi.t_dim_psx_gld_edw_bi where upper(curr_flg)='Y') where rn=1) gld
                   on gld_map.mob_mtched_lylty_id = gld.ulpmembershipid) cust
              on fct.ucic = cust.ucic
            where upper(fct.txn_typ_cd) like 'M%' and nvl(upper(cust.firstname),'') not like 'SHOPPER%'))
  where day_tag = 'gt15'
  group by storecode,bill_date),
booking_sales as
   (select
         storecode
         ,sale_order_no
         ,order_date
         ,order_value
        from
         (select
            store_code as storecode
            ,sale_order_no
            ,order_date
            ,order_value
            ,row_number() over(partition by sale_order_no order by order_date) as rn
           from
            (select
               store_code
               ,sale_order_no
               ,date(createddate) as order_date
               ,sum(netprice) as order_value
              from (select
                      nvl(q.current_storecode,p.store_code) as store_code
                      ,sale_order_no
                      ,createddate
                      ,netprice
                      ,sddocumenttype
                      ,reason_for_rejections
                    from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_stg_ew_isr_trns_saleorder_bi p
                      left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) q
                        on p.store_code = q.old_storecode) a
                left join ds_fld_digit_inboud_db.edw_base_data_bi.t_tgt_ew_isr_mstr_tvakt_salesdoctypemaster_bi b
                  on a.sddocumenttype =b.auart
               where isnull(a.reason_for_rejections,'') in ('0','')
                 and upper(b.SALETYPE) = 'BOOKING SALES'
               group by store_code,sale_order_no,date(createddate)))
           where rn=1),
pending_booking_orders as
   (select
      s1.storecode
      ,s1.bill_date
      ,nvl(pending_order_value,0) as pending_order_value
      ,nvl(str_dlvr_order_value,0) as str_dlvr_order_value
      ,nvl(booking_ordr_value,0) as booking_ordr_value
     from (select distinct storecode,bill_date from fct) s1
       left join
         (select
            storecode
            ,order_date
            ,sum(order_value) as pending_order_value
           from
            (select
                storecode
                ,sale_order_no
                ,order_date
                ,order_value
               from booking_sales
                 left join (select distinct sls_order_num from fct) fct
                   on booking_sales.sale_order_no=fct.sls_order_num
                where fct.sls_order_num is null)
            group by storecode,order_date) o1
        on s1.storecode = o1.storecode
        and s1.bill_date = o1.order_date
       left join
         (select
           storecode
           ,delivrd_date_at_store
           ,sum(order_value) as str_dlvr_order_value
          from
           (select
               nvl(b.current_storecode,a.storecode) as storecode
               ,order_number,order_value,delivrd_date_at_store
             from fielddigital_eyewear.t_tgt_fld_ew_dim_ord_pending_days a
              left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
                on a.storecode = b.old_storecode
              where a.rec_status_cd=1 and a.delivrd_date_at_store is not null) delvr_at_store
           left join (select distinct sls_order_num from fct) fct
             on delvr_at_store.order_number=fct.sls_order_num
          where fct.sls_order_num is null
           group by storecode,delivrd_date_at_store) d1
        on s1.storecode = d1.storecode
        and s1.bill_date = d1.delivrd_date_at_store
       left join
         (select
             storecode
             ,order_date
             ,sum(order_value) as booking_ordr_value
            from booking_sales
              group by storecode,order_date) b1
        on s1.storecode = b1.storecode
        and s1.bill_date = b1.order_date),
store_delivrd_90d_ord as
   (select
      a.storecode
      ,a.bill_date
      ,sum(b.str_dlvr_order_value) as str_dlvr_order_l90d_value
     from pending_booking_orders a, pending_booking_orders b
       where a.storecode=b.storecode
         and b.bill_date<a.bill_date AND b.bill_date>=dateadd(day,-90,a.bill_date) --as per manish 90days should be taken and <bill_date as delay for 1 days and >= to derive for 90 days
      group by a.storecode,a.bill_date),
booking_60d_ord as
   (select
      a.storecode
      ,a.bill_date
      ,sum(b.booking_ordr_value) as booking_ordr_l60d_value
     from pending_booking_orders a, pending_booking_orders b
       where a.storecode=b.storecode
         and b.bill_date<=a.bill_date AND b.bill_date>dateadd(day,-60,a.bill_date) --as per manish latest 60days should be taken and <=bill_date and > to derive for 60 days
      group by a.storecode,a.bill_date),
pending_90d_ord as
   (select
      a.storecode
      ,a.bill_date
      ,sum(b.pending_order_value) as pending_order_l90d_value
     from pending_booking_orders a, pending_booking_orders b
       where a.storecode=b.storecode
         and b.bill_date<=a.bill_date AND b.bill_date>dateadd(day,-90,a.bill_date) --as per manish latest 90days should be taken and <=bill_date and > to derive for 90 days
      group by a.storecode,a.bill_date),
store_stdzn_score as
  (select
      *
     from
      (select
         storecode
         ,score_year
         ,score_month
         ,stdzn_score_monthly
         ,row_number() over(partition by storecode,score_year,score_month order by tgt_flg desc,current_storecode desc) as rn
        from
         (select
             nvl(b.current_storecode,a.storecode) as storecode
             ,case when a.storecode = b.current_storecode then 'Y'
                   else 'N'
              end tgt_flg
             ,current_storecode
             ,score_year
             ,score_month
             ,stdzn_score as stdzn_score_monthly
            from fielddigital_eyewear.t_tgt_fld_ew_dim_store_stdzn a
              left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
               on a.storecode = b.old_storecode
              where a.rec_status_cd = 1)) where rn=1),
---- fecthcing monthly targets
store_targets as
  (select
      *
    from
     (select
         storecode
         ,target_month
         ,target_year
         ,value_target_monthly
         ,b_ds_value_target_monthly
         ,buyers_target_monthly
         ,sg_vol_target_monthly
         ,sg_val_target_monthly
         ,rx_sg_vol_target_monthly
         ,hon_sel_target_monthly
         ,nps_2days_target_monthly
         ,nps_15days_target_monthly
         ,nps_90days_target_monthly
         ,nps_330days_target_monthly
         ,comf_call_target_monthly
         ,stdzn_score_target_monthly
         ,row_number() over(partition by storecode,target_month,target_year order by tgt_flg desc,current_storecode desc) as rn
        from
         (select
            nvl(b.current_storecode,a.storecode) as storecode
            ,case when a.storecode = b.current_storecode then 'Y'
                  else 'N'
             end tgt_flg
            ,current_storecode
            ,target_month
            ,target_year
            ,value_target as value_target_monthly
            ,b_ds_value_target as b_ds_value_target_monthly
            ,buyers_target as buyers_target_monthly
            ,sg_vol_target as sg_vol_target_monthly
            ,sg_val_target as sg_val_target_monthly
            ,rx_sg_vol_target as rx_sg_vol_target_monthly
            ,honest_selling_target as hon_sel_target_monthly
            ,nps_2days_target as nps_2days_target_monthly
            ,nps_15days_target as nps_15days_target_monthly
            ,nps_90days_target as nps_90days_target_monthly
            ,nps_330days_target as nps_330days_target_monthly
            ,comfort_calling_target as comf_call_target_monthly
            ,stdzn_score as stdzn_score_target_monthly
           from fielddigital_eyewear.t_tgt_fld_ew_dim_store_target a
             left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
              on a.storecode = b.old_storecode
             where a.rec_status_cd = 1)) where rn=1)
select
   bills_buyers.storecode
   ,bills_buyers.bill_date
   ,no_of_bills
   ,nvl(mtd_buyers.no_of_buyers_mtd,0) as no_of_buyers_mtd
   ,nvl(ytd_buyers.no_of_buyers_ytd,0) as no_of_buyers_ytd
   ,value
   ,case when bills_buyers.storecode in ('IWMV','IDSS','IWMM','TKSS') then 0
         else nvl(sale_vol.volume,0)
    end as volume
   ,nvl(bds_sales.bds_value,0) as bds_value
   ,nvl(bds_sales.no_of_bds_buyers,0) as no_of_bds_buyers
   ,nvl(mtd_bds_buyers.no_of_bds_buyers_mtd,0) as no_of_bds_buyers_mtd --extra kpi
   ,nvl(ytd_bds_buyers.no_of_bds_buyers_ytd,0) as no_of_bds_buyers_ytd --extra kpi
   ,nvl(sunglass.sung_volume,0) as sung_volume
   ,nvl(sunglass.sung_value,0) as sung_value
   ,nvl(rx_sung.rx_sung_vol,0) as rx_sung_vol
   ,nvl(spec_ticket.spec_no_of_bills,0) as spec_no_of_bills
   ,nvl(spec_ticket.spec_value,0) as spec_value
   ------outside rx
   ,nvl(rx_test.alltype_examin_value,0) as alltype_examin_value
   ,nvl(rx_test.outside_rx_value,0) as outside_rx_value
   ,case when bills_buyers.storecode in ('IWMV','IDSS','IWMM','TKSS') then 0
         else nvl(no_of_customers,0)
    end as no_of_customers
   ----pending order KPIs
   ,nvl(store_delivrd_90d_ord.str_dlvr_order_l90d_value,0) as str_dlvr_order_l90d_value
   ,nvl(booking_60d_ord.booking_ordr_l60d_value,0) as booking_ordr_l60d_value
   ,60 as booking_days_cutoff  ----for 60 days booking average
   ,nvl(pending_90d_ord.pending_order_l90d_value,0) as pending_order_l90d_value
   ------walkins count
   ,nvl(walkins.walkin_cnt,0) as walkin_cnt
   ------selling score columns
   ,nvl(opp_corr_upsell_score,0) as opp_corr_upsell_score
   ,nvl(acc_upsell_score,0) as acc_upsell_score
   ,nvl(unid_score,0) as unid_score
   ,nvl(hon_sell_score,0) as hon_sell_score
   ,nvl(pot_dis_hon_sell_score,0) as pot_dis_hon_sell_score
   ------comfort calling columns
   ,nvl(comfort_calling.tot_comf_call_surveys,0) as tot_comf_call_surveys
   ,nvl(comfort_calling.comf_call_yes_cnt,0) as comf_call_yes_cnt
   ,nvl(comfort_calling.comf_call_no_cnt,0) as comf_call_no_cnt
   ------eyetest exp columns
   ,nvl(eyetest_exp.tot_eyetest_responses,0) as tot_eyetest_responses
   ,nvl(eyetest_exp.eyetest_promot_cnt,0) as eyetest_promot_cnt
   ,nvl(eyetest_exp.eyetest_passiv_cnt,0) as eyetest_passiv_cnt
   ,nvl(eyetest_exp.eyetest_detract_cnt,0) as eyetest_detract_cnt
   ------product variety columns
   ,nvl(tot_prd_vrty_responses,0) as tot_prd_vrty_responses
   ,nvl(prd_vrty_promot_cnt,0) as prd_vrty_promot_cnt
   ,nvl(prd_vrty_passiv_cnt,0) as prd_vrty_passiv_cnt
   ,nvl(prd_vrty_detract_cnt,0) as prd_vrty_detract_cnt
   ------ease of billing columns
   ,nvl(tot_ease_billing_responses,0) as tot_ease_billing_responses
   ,nvl(ease_billing_promot_cnt,0) as ease_billing_promot_cnt
   ,nvl(ease_billing_passiv_cnt,0) as ease_billing_passiv_cnt
   ,nvl(ease_billing_detract_cnt,0) as ease_billing_detract_cnt
   ------On time delivery
   ,nvl(tot_on_time_delivr_responses,0) as tot_on_time_delivr_responses
   ,nvl(on_time_delivr_promot_cnt,0) as on_time_delivr_promot_cnt
   ,nvl(on_time_delivr_passiv_cnt,0) as on_time_delivr_passiv_cnt
   ,nvl(on_time_delivr_detract_cnt,0) as on_time_delivr_detract_cnt
   ------nps surveys
   ,nvl(no_of_nps_surveys,0) as no_of_nps_surveys
   ------2days nps columns
   ,nvl(no_of_responses_2days,0) as no_of_responses_2days
   ,nvl(promoters_cnt_2days,0) as promoters_cnt_2days
   ,nvl(passives_cnt_2days,0) as passives_cnt_2days
   ,nvl(detractors_cnt_2days,0) as detractors_cnt_2days
   ------15days nps columns
   ,nvl(no_of_responses_15days,0) as no_of_responses_15days
   ,nvl(promoters_cnt_15days,0) as promoters_cnt_15days
   ,nvl(passives_cnt_15days,0) as passives_cnt_15days
   ,nvl(detractors_cnt_15days,0) as detractors_cnt_15days
   ------90days nps columns
   ,nvl(no_of_responses_90days,0) as no_of_responses_90days
   ,nvl(promoters_cnt_90days,0) as promoters_cnt_90days
   ,nvl(passives_cnt_90days,0) as passives_cnt_90days
   ,nvl(detractors_cnt_90days,0) as detractors_cnt_90days
   ------330days nps columns
   ,nvl(no_of_responses_330days,0) as no_of_responses_330days
   ,nvl(promoters_cnt_330days,0) as promoters_cnt_330days
   ,nvl(passives_cnt_330days,0) as passives_cnt_330days
   ,nvl(detractors_cnt_330days,0) as detractors_cnt_330days
   ------GMB reviews data
   ,nvl(gmb_reviews.no_of_reviews,0) as no_of_reviews
   ,nvl(gmb_reviews.agg_rating,0) as agg_rating
   ------Store Standardization score
   ,nvl(store_stdzn_score.stdzn_score_monthly,0) as stdzn_score_monthly
   ------store targets
   ,nvl(store_targets.value_target_monthly,0)*100000 as value_target_monthly
   ,nvl(store_targets.b_ds_value_target_monthly,0)*100000 as b_ds_value_target_monthly
   ,nvl(store_targets.buyers_target_monthly,0) as buyers_target_monthly
   ,nvl(store_targets.sg_vol_target_monthly,0) as sg_vol_target_monthly
   ,nvl(store_targets.sg_val_target_monthly,0)*100000 as sg_val_target_monthly
   ,nvl(store_targets.rx_sg_vol_target_monthly,0) as rx_sg_vol_target_monthly
   ,nvl(store_targets.hon_sel_target_monthly,0) as hon_sel_target_monthly
   ,nvl(store_targets.nps_2days_target_monthly,0) as nps_2days_target_monthly
   ,nvl(store_targets.nps_15days_target_monthly,0) as nps_15days_target_monthly
   ,nvl(store_targets.nps_90days_target_monthly,0) as nps_90days_target_monthly
   ,nvl(store_targets.nps_330days_target_monthly,0) as nps_330days_target_monthly
   ,nvl(store_targets.comf_call_target_monthly,0) as comf_call_target_monthly
   ,nvl(store_targets.stdzn_score_target_monthly,0) as stdzn_score_target_monthly
   ,abm_name
   ,abm_mailid
   ,ltl
   ,status
   ,storecity
   ,storeregion
   ,country
   ,channel
   ,divisionname
   ,open_date
   ,store_age
   ,benchmark_group
   ,combo_status
   ,mall_status
   ,owner_type
   ,rbm_name
   ,rbm_mailid
  from bills_buyers
    left join bds_sales
     on bills_buyers.storecode = bds_sales.storecode
     and bills_buyers.bill_date = bds_sales.order_date
    left join sunglass
     on bills_buyers.storecode = sunglass.storecode
     and bills_buyers.bill_date = sunglass.bill_date
    left join rx_sung
     on bills_buyers.storecode = rx_sung.storecode
     and bills_buyers.bill_date = rx_sung.bill_date
    left join spec_ticket
     on bills_buyers.storecode = spec_ticket.storecode
     and bills_buyers.bill_date = spec_ticket.bill_date
    left join no_of_customers
     on bills_buyers.storecode = no_of_customers.storecode
     and bills_buyers.bill_date = no_of_customers.bill_date
    left join selling_scores
     on bills_buyers.storecode = selling_scores.storecode
     and bills_buyers.bill_date = selling_scores.bill_date
    left join comfort_calling
     on bills_buyers.storecode = comfort_calling.storecode
     and bills_buyers.bill_date = comfort_calling.bill_date
    left join eyetest_exp
     on bills_buyers.storecode = eyetest_exp.storecode
     and bills_buyers.bill_date = eyetest_exp.bill_date
    left join prodct_variety
     on bills_buyers.storecode = prodct_variety.storecode
     and bills_buyers.bill_date = prodct_variety.bill_date
    left join ease_billing
     on bills_buyers.storecode = ease_billing.storecode
     and bills_buyers.bill_date = ease_billing.bill_date
    left join on_time_delivr
     on bills_buyers.storecode = on_time_delivr.storecode
     and bills_buyers.bill_date = on_time_delivr.bill_date
    left join nps_surveys
     on bills_buyers.storecode = nps_surveys.storecode
     and bills_buyers.bill_date = nps_surveys.bill_date
    left join litmus_2days
     on bills_buyers.storecode = litmus_2days.storecode
     and bills_buyers.bill_date = litmus_2days.request_date
    left join litmus_15days
     on bills_buyers.storecode = litmus_15days.storecode
     and bills_buyers.bill_date = litmus_15days.request_date
    left join litmus_90days
     on bills_buyers.storecode = litmus_90days.storecode
     and bills_buyers.bill_date = litmus_90days.request_date
    left join litmus_330days
     on bills_buyers.storecode = litmus_330days.storecode
     and bills_buyers.bill_date = litmus_330days.request_date
    left join gmb_reviews
     on bills_buyers.storecode = gmb_reviews.storecode
     and bills_buyers.bill_date = gmb_reviews.review_date
    left join walkins
     on bills_buyers.storecode = walkins.storecode
     and bills_buyers.bill_date = walkins.shoperdate
    left join rx_test
     on bills_buyers.storecode = rx_test.storecode
     and bills_buyers.bill_date = rx_test.bill_date
    left join store_delivrd_90d_ord
     on bills_buyers.storecode = store_delivrd_90d_ord.storecode
     and bills_buyers.bill_date = store_delivrd_90d_ord.bill_date
    left join booking_60d_ord
     on bills_buyers.storecode = booking_60d_ord.storecode
     and bills_buyers.bill_date = booking_60d_ord.bill_date
    left join pending_90d_ord
     on bills_buyers.storecode = pending_90d_ord.storecode
     and bills_buyers.bill_date = pending_90d_ord.bill_date
    left join sale_vol
     on bills_buyers.storecode = sale_vol.storecode
     and bills_buyers.bill_date = sale_vol.bill_date
    left join store_targets
     on bills_buyers.storecode = store_targets.storecode
     and date_part(month, bills_buyers.bill_date) = store_targets.target_month
     and date_part(year, bills_buyers.bill_date) = store_targets.target_year
    left join store_stdzn_score
     on bills_buyers.storecode = store_stdzn_score.storecode
     and date_part(month, bills_buyers.bill_date) = store_stdzn_score.score_month
     and date_part(year, bills_buyers.bill_date) = store_stdzn_score.score_year
    left join mtd_buyers
     on bills_buyers.storecode = mtd_buyers.storecode
     and date_part(month, bills_buyers.bill_date) = mtd_buyers.bill_month
     and date_part(year, bills_buyers.bill_date) = mtd_buyers.bill_year
    left join ytd_buyers
     on bills_buyers.storecode = ytd_buyers.storecode
     and DATE_PART(year,(DATEADD(month,-3,bills_buyers.bill_date))) = ytd_buyers.fin_year
    left join mtd_bds_buyers
     on bills_buyers.storecode = mtd_bds_buyers.storecode
     and date_part(month, bills_buyers.bill_date) = mtd_bds_buyers.bill_month
     and date_part(year, bills_buyers.bill_date) = mtd_bds_buyers.bill_year
    left join ytd_bds_buyers
     on bills_buyers.storecode = ytd_bds_buyers.storecode
     and DATE_PART(year,(DATEADD(month,-3,bills_buyers.bill_date))) = ytd_bds_buyers.fin_year;

--03. mv_ew_priceband_sunglass_performance
create materialized view fielddigital_eyewear.mv_ew_priceband_sunglass_performance
as
select
   storecode
   ,bill_date
   ,price_band
   ,sum(volume) as volume
   ,sum(value) as value
   ,abm_name
   ,abm_mailid
   ,ltl
   ,status
   ,storecity
   ,storeregion
   ,country
   ,channel
   ,divisionname
   ,store_age
   ,open_date
   ,benchmark_group
  from
    (select
        storecode
        ,bill_date
        ,rtl_qty as volume
        ,itm_cd
        ,case when abs(ucp_val) between 0 and 1500 then '0-1.5k'
              when abs(ucp_val) between 1500 and 3000 then '>1.5-3k'
              when abs(ucp_val) between 3000 and 5000 then '>3-5k'
              when abs(ucp_val) between 5000 and 8000 then '>5-8k'
              when abs(ucp_val) between 8000 and 15000 then '>8-15k'
              when abs(ucp_val) > 15000 then '>15k'
         end as price_band
        ,nucp_val as value
        ,abm_name
        ,abm_mailid
        ,ltl
        ,status
        ,storecity
        ,storeregion
        ,country
        ,channel
        ,divisionname
        ,store_age
        ,open_date
        ,benchmark_group
       from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg
         where upper(cat_nm) like '%SUNGLASS%' and upper(mer_cat)<>'CUS-FRAME')
         group by storecode,bill_date,price_band,abm_name,abm_mailid,ltl,status,storecity,storeregion,country,channel,divisionname,store_age,open_date,benchmark_group;

--04. mv_ew_priceband_spectacle_performance
create materialized view fielddigital_eyewear.mv_ew_priceband_spectacle_performance
as
---fecthes invoices with only frames and lens and spectacle logic count
with fct as
  (select
      storecode
      ,bill_date
      ,inv_num
      ,ucic
      ,order_typ
      ,pr_examin_by
      ,brnd_nm
      ,cat_nm
      ,cust_code
      ,txn_typ_cd
      ,itm_cd
      ,mer_cat
      ,spec_tag
      ,rtl_qty
      ,sls_order_num
      ,pos_inv_num
      ,ucp_val
      ,gucp_val
      ,nucp_val
      ,sale_per
      ,abm_name
      ,abm_mailid
      ,ltl
      ,status
      ,storecity
      ,storeregion
      ,country
      ,channel
      ,divisionname
      ,store_age
      ,open_date
      ,benchmark_group
     from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg),
spects as
  (select
          storecode
          ,bill_date
          ,inv_num
          ,sum(nucp_val) as spec_value
          ,sum(abs(ucp_val))/(sum(abs(rtl_qty))/3) as spec_ucp_val
          ,sum(rtl_qty)/3 as spec_volume
          ,abm_name
          ,abm_mailid
          ,ltl
          ,status
          ,storecity
          ,storeregion
          ,country
          ,channel
          ,divisionname
          ,store_age
          ,open_date
          ,benchmark_group
         from fct
          where upper(spec_tag)='SR'
           group by storecode,bill_date,inv_num,abm_name,abm_mailid,ltl,status,storecity,storeregion,country,channel,divisionname,open_date,store_age,benchmark_group)
select
   storecode
   ,bill_date
   ,price_band
   ,sum(spec_volume) as volume
   ,sum(spec_value) as value
   ,abm_name
   ,abm_mailid
   ,ltl
   ,status
   ,storecity
   ,storeregion
   ,country
   ,channel
   ,divisionname
   ,store_age
   ,open_date
   ,benchmark_group
  from
    (select
        storecode
        ,bill_date
        ,inv_num
        ,case  when  spec_ucp_val between 0 and 1500 then '0-1.5k'
               when  spec_ucp_val between 1500 and 3000 then '>1.5-3k'
               when  spec_ucp_val between 3000 and 6000 then '>3-6k'
               when  spec_ucp_val between 6000 and 8000 then '>6-8k'
               when  spec_ucp_val between 8000 and 15000 then '>8-15k'
               when  spec_ucp_val > 15000 then '>15k'
         end as price_band
        ,spec_volume
        ,spec_value
        ,abm_name
        ,abm_mailid
        ,ltl
        ,status
        ,storecity
        ,storeregion
        ,country
        ,channel
        ,divisionname
        ,store_age
        ,open_date
        ,benchmark_group
       from spects)
   group by storecode,bill_date,price_band,abm_name,abm_mailid,ltl,status,storecity,storeregion,country,channel,divisionname,store_age,open_date,benchmark_group;

--05. mv_ew_category_lens_analysis
create materialized view fielddigital_eyewear.mv_ew_category_lens_analysis
as
select
  storecode
  ,bill_date
  ,category
  ,sum(rtl_qty) as volume
  ,sum(nucp_val) as value
  ,count(distinct (case when upper(txn_typ_cd) like 'M%' then storecode||bill_date||inv_num end))+
   count(distinct (case when upper(txn_typ_cd) like 'S%' then storecode||bill_date||inv_num end))-
   count(distinct (case when upper(txn_typ_cd) like 'N%' then storecode||bill_date||inv_num end))-
   count(distinct (case when upper(txn_typ_cd) like 'O%' then storecode||bill_date||inv_num end)) as no_of_bills
  ,abm_name
  ,abm_mailid
  ,ltl
  ,status
  ,storecity
  ,storeregion
  ,country
  ,channel
  ,divisionname
  ,store_age
  ,open_date
  ,benchmark_group
 from
  (select
     storecode
     ,bill_date
     ,inv_num
     ,txn_ln_num
     ,case
       when upper(brnd_grp_cd)='HB' and upper(cat_nm) like '%FRAMES%' then 'HB Frames'
       when upper(brnd_grp_cd)='OB' and upper(cat_nm) like '%FRAMES%' then 'IB Frames'
       when upper(brnd_grp_cd)='HB' and upper(cat_nm) like '%LENS%' and upper(cat_nm)<>'CONTACT LENSES' then 'HB Lens'
       when upper(brnd_grp_cd)='OB' and upper(cat_nm) like '%LENS%' and upper(cat_nm)<>'CONTACT LENSES' then 'IB Lens'
       when upper(brnd_grp_cd)='HB' and upper(cat_nm) like '%SUNGLASS%' then 'HB Sunglasses'
       when upper(brnd_grp_cd)='OB' and upper(cat_nm) like '%SUNGLASS%' then 'IB Sunglasses'
       when upper(cat_nm) = 'CONTACT LENSES' then 'Contact Lens'
       else 'Others'
     end as category
     ,rtl_qty
     ,nucp_val
     ,txn_typ_cd
     ,abm_name
     ,abm_mailid
     ,ltl
     ,status
     ,storecity
     ,storeregion
     ,country
     ,channel
     ,divisionname
     ,store_age
     ,open_date
     ,benchmark_group
    from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg
      where nvl(upper(mer_cat),'') not in ('CUS-LENS','CUS-FRAME'))
  group by storecode,bill_date,category,abm_name,abm_mailid,ltl,status,storecity,storeregion,country,channel,divisionname,open_date,store_age,benchmark_group;

--06. mv_ew_overall_lens_analysis
create materialized view fielddigital_eyewear.mv_ew_overall_lens_analysis
as
select
   storecode
   ,bill_date
   ,singlevision_volume
   ,singlevision_value
   ,bifocal_volume
   ,bifocal_value
   ,progressive_volume
   ,progressive_value
   ,fsv_lens_volume
   ,fsv_lens_value
   ,total_lens_volume
   ,total_lens_value
   ,singlevision_no_of_bills
   ,bifocal_no_of_bills
   ,progressive_no_of_bills
   ,fsv_lens_no_of_bills
   ,total_lens_no_of_bills
   ,abm_name
   ,abm_mailid
   ,ltl
   ,status
   ,storecity
   ,storeregion
   ,country
   ,channel
   ,divisionname
   ,store_age
   ,open_date
   ,benchmark_group
  from
   (select
       storecode
       ,bill_date
       ,sum(case when upper(lens_category) ='SV RX'  then rtl_qty else 0 end ) as singlevision_volume
       ,sum(case when upper(lens_category) ='SV RX'  then nucp_val else 0 end ) as singlevision_value
       ,sum(case when upper(lens_category)='BIFOCAL'  then rtl_qty else 0 end ) as bifocal_volume
       ,sum(case when upper(lens_category) ='BIFOCAL'  then nucp_val else 0 end ) as bifocal_value
       ,sum(case when upper(lens_category) ='PROGRESSIVE'  then rtl_qty else 0 end ) as progressive_volume
       ,sum(case when upper(lens_category) ='PROGRESSIVE'  then nucp_val else 0 end ) as progressive_value
       ,sum(case when upper(lens_category) ='FSV'  then rtl_qty else 0 end ) as fsv_lens_volume
       ,sum(case when upper(lens_category) ='FSV'  then nucp_val else 0 end ) as fsv_lens_value
       ,sum(case when lower(cat_nm) like '%lens%' and lower(cat_nm)<>'contact lenses' then rtl_qty else 0 end ) as total_lens_volume
       ,sum(case when lower(cat_nm) like '%lens%' and lower(cat_nm)<>'contact lenses' then nucp_val else 0 end ) as total_lens_value
       ,count(distinct (case when upper(txn_typ_cd) like 'M%' and upper(lens_category)='SV RX' then storecode||bill_date||inv_num end))+
        count(distinct (case when upper(txn_typ_cd) like 'S%' and upper(lens_category)='SV RX'  then storecode||bill_date||inv_num end))-
        count(distinct (case when upper(txn_typ_cd) like 'N%' and upper(lens_category)='SV RX'  then storecode||bill_date||inv_num end))-
        count(distinct (case when upper(txn_typ_cd) like 'O%' and upper(lens_category)='SV RX'  then storecode||bill_date||inv_num end)) as singlevision_no_of_bills
       ,count(distinct (case when upper(txn_typ_cd) like 'M%' and upper(lens_category)='BIFOCAL' then storecode||bill_date||inv_num end))+
        count(distinct (case when upper(txn_typ_cd) like 'S%' and upper(lens_category)='BIFOCAL' then storecode||bill_date||inv_num end))-
        count(distinct (case when upper(txn_typ_cd) like 'N%' and upper(lens_category)='BIFOCAL' then storecode||bill_date||inv_num end))-
        count(distinct (case when upper(txn_typ_cd) like 'O%' and upper(lens_category)='BIFOCAL' then storecode||bill_date||inv_num end)) as bifocal_no_of_bills
       ,count(distinct (case when upper(txn_typ_cd) like 'M%' and upper(lens_category)='PROGRESSIVE' then storecode||bill_date||inv_num end))+
        count(distinct (case when upper(txn_typ_cd) like 'S%' and upper(lens_category)='PROGRESSIVE' then storecode||bill_date||inv_num end))-
        count(distinct (case when upper(txn_typ_cd) like 'N%' and upper(lens_category)='PROGRESSIVE' then storecode||bill_date||inv_num end))-
        count(distinct (case when upper(txn_typ_cd) like 'O%' and upper(lens_category)='PROGRESSIVE' then storecode||bill_date||inv_num end)) as progressive_no_of_bills
       ,count(distinct (case when upper(txn_typ_cd) like 'M%' and upper(lens_category)='FSV' then storecode||bill_date||inv_num end))+
        count(distinct (case when upper(txn_typ_cd) like 'S%' and upper(lens_category)='FSV' then storecode||bill_date||inv_num end))-
        count(distinct (case when upper(txn_typ_cd) like 'N%' and upper(lens_category)='FSV' then storecode||bill_date||inv_num end))-
        count(distinct (case when upper(txn_typ_cd) like 'O%' and upper(lens_category)='FSV' then storecode||bill_date||inv_num end)) as fsv_lens_no_of_bills
       ,count(distinct (case when upper(txn_typ_cd) like 'M%' and lower(cat_nm) like '%lens%' and lower(cat_nm)<>'contact lenses' then storecode||bill_date||inv_num end))+
        count(distinct (case when upper(txn_typ_cd) like 'S%' and lower(cat_nm) like '%lens%' and lower(cat_nm)<>'contact lenses' then storecode||bill_date||inv_num end))-
        count(distinct (case when upper(txn_typ_cd) like 'N%' and lower(cat_nm) like '%lens%' and lower(cat_nm)<>'contact lenses' then storecode||bill_date||inv_num end))-
        count(distinct (case when upper(txn_typ_cd) like 'O%' and lower(cat_nm) like '%lens%' and lower(cat_nm)<>'contact lenses' then storecode||bill_date||inv_num end)) as total_lens_no_of_bills
       ,abm_name
       ,abm_mailid
       ,ltl
       ,status
       ,storecity
       ,storeregion
       ,country
       ,channel
       ,divisionname
       ,store_age
       ,open_date
       ,benchmark_group
       from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg
         where nvl(upper(mer_cat),'') not in ('CUS-LENS','CUS-FRAME')
        group by storecode,bill_date,abm_name,abm_mailid,ltl,status,storecity,storeregion,country,channel,divisionname,open_date,store_age,benchmark_group);

--07. mv_ew_customer_movement
create materialized view fielddigital_eyewear.mv_ew_customer_movement
as
with
fct as
  (select distinct
     storecode
     ,extract(year from bill_date) as bill_year
     ,extract(month from bill_date) as bill_month
     ,abm_name
     ,abm_mailid
     ,ltl
     ,status
     ,storecity
     ,storeregion
     ,country
     ,channel
     ,divisionname
     ,open_date
     ,store_age
     ,benchmark_group
     ,combo_status
     ,mall_status
   from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg
    where bill_date>=date_trunc('month',(select max(bill_date) from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg))),
enrld_store as
(select
   posidex_unified_id
   ,enrld_storecode
  from
   (select
     enrld_storecode
     ,posidex_unified_id
     ,first_txn_date
     ,row_number() over(partition by posidex_unified_id order by first_txn_date) as rn
    from
     (select
        loc_code as enrld_storecode
        ,posidex_unified_id
        ,min(date(inv_dat)) as first_txn_date
       from (select
                nvl(b.current_storecode,a.loc_code) as loc_code
                ,posidex_unified_id
                ,bill_type
                ,inv_dat
              from ds_fld_digit_inboud_db.etl_wrk_data_bi.stg_isr_interim_bi a
                left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
                 on a.loc_code = b.old_storecode)
        where nvl(upper(bill_type),'') not in (select distinct upper(bill_type) from fielddigital_eyewear.t_tgt_ew_isr_dim_billtype_exclude)
         group by loc_code,posidex_unified_id))
   where rn=1),
cust_enrld as
  (select distinct
        storecode
        ,bill_date
        ,gld_map.mob_mtched_lylty_id
        ,enrld_store.enrld_storecode
       from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg stg
        inner join ds_fld_digit_inboud_db.sem_pu_cust_all.t_tgt_cust_all_ucic_lylty_mapng_bi gld_map
          on stg.ucic = gld_map.ucic
        inner join enrld_store
          on stg.ucic = enrld_store.posidex_unified_id
     where stg.ucic<>0 and stg.bill_date>=dateadd('month',-12,date_trunc('month',(select max(bill_date) from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg)))),
moved_in_mnth as
  (select
    storecode
    ,movedin_from_storecode_mtd
    ,bill_year
    ,bill_month
    ,moved_in_cnt_mtd
   from
    (select
        storecode
        ,movedin_from_storecode_mtd
        ,bill_year
        ,bill_month
        ,moved_in_cnt_mtd
        ,row_number() over(partition by storecode,bill_year,bill_month order by moved_in_cnt_mtd desc) as rn
      from
        (select
         storecode
         ,enrld_storecode as movedin_from_storecode_mtd
         ,extract(year from bill_date) as bill_year
         ,extract(month from bill_date) as bill_month
         ,count(distinct mob_mtched_lylty_id) as moved_in_cnt_mtd
        from cust_enrld
         where storecode<>enrld_storecode and bill_date>=date_trunc('month',(select max(bill_date) from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg))
          group by storecode,bill_year,bill_month,enrld_storecode))
          where rn<=5),
moved_in_year as
  (select
     storecode
     ,count(distinct mob_mtched_lylty_id) as moved_in_cnt_year
    from cust_enrld
     where storecode<>enrld_storecode and bill_date>=dateadd('month',-12,date_trunc('month',(select max(bill_date) from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg)))
      group by storecode),
moved_out_mnth as
  (select
     enrld_storecode
     ,movedout_to_storecode_mtd
     ,bill_year
     ,bill_month
     ,moved_out_cnt_mtd
    from
     (select
        enrld_storecode
        ,movedout_to_storecode_mtd
        ,bill_year
        ,bill_month
        ,moved_out_cnt_mtd
        ,row_number() over(partition by enrld_storecode,bill_year,bill_month order by moved_out_cnt_mtd desc) as rn
       from
        (select
           enrld_storecode
           ,storecode as movedout_to_storecode_mtd
           ,extract(year from bill_date) as bill_year
           ,extract(month from bill_date) as bill_month
           ,count(distinct mob_mtched_lylty_id) as moved_out_cnt_mtd
          from cust_enrld
           where storecode<>enrld_storecode and bill_date>=date_trunc('month',(select max(bill_date) from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg))
            group by enrld_storecode,bill_year,bill_month,storecode))
    where rn<=5),
moved_out_year as
  (select
     enrld_storecode
     ,count(distinct mob_mtched_lylty_id) as moved_out_cnt_year
    from cust_enrld
     where storecode<>enrld_storecode and bill_date>=dateadd('month',-12,date_trunc('month',(select max(bill_date) from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg)))
      group by enrld_storecode)
select
 fct.storecode
 ,fct.bill_year
 ,fct.bill_month
 ,(select max(bill_date) from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg) as refresh_date
 ,nvl(moved_in_cnt_mtd,0) as moved_in_cnt_mtd
 ,nvl(moved_in_cnt_year,0) as moved_in_cnt_year
 ,nvl(moved_out_cnt_mtd,0) as moved_out_cnt_mtd
 ,nvl(moved_out_cnt_year,0) as moved_out_cnt_year
 ,movedin_from_storecode_mtd
 ,movedout_to_storecode_mtd
 ,abm_name
 ,abm_mailid
 ,ltl
 ,status
 ,storecity
 ,storeregion
 ,country
 ,channel
 ,divisionname
 ,open_date
 ,store_age
 ,benchmark_group
 ,combo_status
 ,mall_status
from fct
  left join moved_in_mnth
   on fct.storecode = moved_in_mnth.storecode
   and fct.bill_year = moved_in_mnth.bill_year
   and fct.bill_month = moved_in_mnth.bill_month
  left join moved_in_year
    on fct.storecode = moved_in_year.storecode
  left join moved_out_mnth
   on fct.storecode = moved_out_mnth.enrld_storecode
   and fct.bill_year = moved_out_mnth.bill_year
   and fct.bill_month = moved_out_mnth.bill_month
  left join moved_out_year
    on fct.storecode = moved_out_year.enrld_storecode;

--8. mv_ew_stock_volume_master
create materialized view fielddigital_eyewear.mv_ew_stock_volume_master
as
with
store_master as
(select
    storecode
    ,abm_name
    ,abm_mailid
    ,ltl
    ,status
    ,storecity
    ,storeregion
    ,country
    ,channel
    ,divisionname
    ,open_date
    ,combo_status
    ,mall_status
   from fielddigital_eyewear.t_tgt_fld_ew_dim_store_master
    where rec_status_cd=1 and storecode not in (select distinct old_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping 
														  where rec_status_cd=1 and current_storecode <> old_storecode)),
stock_vol_master as
  (select
      *
     from
      (select
         storecode
         ,stock_date
         ,kpi
         ,norms
         ,actuals
         ,ams_past_3months
         ,stock_cover
         ,alignment_percent
         ,dense_rank() over(partition by storecode order by stock_date desc) as rn
        from
         (select
             nvl(b.current_storecode,a.storecode) as storecode
--             ,case when a.storecode = b.current_storecode then 'Y'
--                   else 'N'
--              end tgt_flg
             ,current_storecode
             ,stock_date
             ,kpi
             ,norms
             ,actuals
             ,ams_past_3months
             ,stock_cover
             ,alignment_percent
            from fielddigital_eyewear.t_tgt_fld_ew_dim_stock_vol_master a
              left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1 and current_storecode <> old_storecode) b
               on a.storecode = b.old_storecode
             where a.rec_status_cd=1 and a.stock_date>=dateadd('year',-1,dateadd(month,3,date_trunc('year',dateadd(month,-3,sysdate))))))
      where rn=1) -- get only latest date records [updated by sanjeeva on 2023-12-19]
select
   store_master.storecode
   ,stock_date
   ,kpi
   ,norms
   ,actuals
   ,ams_past_3months
   ,stock_cover
   ,alignment_percent
   ,abm_name
   ,abm_mailid
   ,ltl
   ,status
   ,storecity
   ,storeregion
   ,country
   ,channel
   ,divisionname
   ,open_date
   ,combo_status
   ,mall_status
  from store_master
    inner join stock_vol_master
      on store_master.storecode = stock_vol_master.storecode;

--9. mv_ew_stock_value_master
create materialized view fielddigital_eyewear.mv_ew_stock_value_master
as
with
store_master as
(select
    storecode
    ,abm_name
    ,abm_mailid
    ,ltl
    ,status
    ,storecity
    ,storeregion
    ,country
    ,channel
    ,divisionname
    ,open_date
    ,combo_status
    ,mall_status
   from fielddigital_eyewear.t_tgt_fld_ew_dim_store_master
    where rec_status_cd=1 and storecode not in (select distinct old_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping 
														  where rec_status_cd=1 and current_storecode <> old_storecode)),
stock_val_master as
  (select
      *
     from
      (select
         storecode
         ,stock_date
         ,kpi
         ,norms
         ,actuals
         ,ams_past_3months
         ,stock_cover
         ,alignment_percent
         ,dense_rank() over(partition by storecode order by stock_date desc) as rn	-- Maintain only latest date records [2023-12-19]
        from
         (select
             nvl(b.current_storecode,a.storecode) as storecode
--             ,case when a.storecode = b.current_storecode then 'Y'
--                   else 'N'
--              end tgt_flg
             ,current_storecode
             ,stock_date
             ,kpi
             ,norms
             ,actuals
             ,ams_past_3months
             ,stock_cover
             ,alignment_percent
            from fielddigital_eyewear.t_tgt_fld_ew_dim_stock_val_master a
              left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1 and current_storecode <> old_storecode) b
               on a.storecode = b.old_storecode
             where a.rec_status_cd=1 and a.stock_date>=dateadd('year',-1,dateadd(month,3,date_trunc('year',dateadd(month,-3,sysdate))))))
      where rn=1)
select
   store_master.storecode
   ,stock_date
   ,kpi
   ,norms
   ,actuals
   ,ams_past_3months
   ,stock_cover
   ,alignment_percent
   ,abm_name
   ,abm_mailid
   ,ltl
   ,status
   ,storecity
   ,storeregion
   ,country
   ,channel
   ,divisionname
   ,open_date
   ,combo_status
   ,mall_status
  from store_master
    inner join stock_val_master
      on store_master.storecode = stock_val_master.storecode;

--10. mv_ew_buyer_details

create materialized view fielddigital_eyewear.mv_ew_buyer_details as
with sales_cte as (
select storecode
    ,bill_date
    ,abm_name
    ,abm_mailid
    ,ltl
    ,status
    ,storecity
    ,storeregion
    ,country
    ,open_date
    ,store_age
    ,benchmark_group
    ,mapp.mob_mtched_lylty_id
    ,cust_code
    ,count(distinct (case when upper(txn_typ_cd) like 'M%' then storecode||bill_date||inv_num end))+
       count(distinct (case when upper(txn_typ_cd) like 'S%' then storecode||bill_date||inv_num end))-
       count(distinct (case when upper(txn_typ_cd) like 'N%' then storecode||bill_date||inv_num end))-
       count(distinct (case when upper(txn_typ_cd) like 'O%' then storecode||bill_date||inv_num end)) as cust_no_of_bills
    ,sum(nucp_val) as cust_bill_value
    from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg sales
    left join ds_fld_digit_inboud_db.sem_pu_cust_all.t_tgt_cust_all_ucic_lylty_mapng_bi mapp
        on sales.ucic = mapp.ucic
    where upper(bill_type) not in (select distinct upper(bill_type) from fielddigital_eyewear.t_tgt_ew_isr_dim_billtype_exclude)
    group by storecode
    ,bill_date
    ,abm_name
    ,abm_mailid
    ,ltl
    ,status
    ,storecity
    ,storeregion
    ,country
    ,open_date
    ,store_age
    ,benchmark_group
    ,mapp.mob_mtched_lylty_id
    ,cust_code)
, visit_cte as (
select *
    ,row_number() over (partition by cust_code order by bill_date) visit
    ,lag(bill_date) over (partition by cust_code order by bill_date) prev_bill_date
    from (
    select distinct mob_mtched_lylty_id, cust_code
        ,date(inv_dat) as bill_date
        from ds_fld_digit_inboud_db.etl_wrk_data_bi.stg_isr_interim_bi sales
    left join ds_fld_digit_inboud_db.sem_pu_cust_all.t_tgt_cust_all_ucic_lylty_mapng_bi mapp
        on sales.posidex_unified_id = mapp.ucic
    --where upper(bill_type) not in (select distinct upper(bill_type) from fielddigital_eyewear.t_tgt_ew_isr_dim_billtype_exclude)
    )
)
select storecode
    ,bill_date
    ,abm_name
    ,abm_mailid
    ,ltl
    ,status
    ,storecity
    ,storeregion
    ,country
    ,open_date
    ,store_age
    ,benchmark_group
    ,mob_mtched_lylty_id
    ,cust_code
    ,cust_no_of_bills
    ,cust_bill_value
    --,case when extract(month from bill_date) = extract(month from prev_bill_date) and extract(year from bill_date) = extract(year from prev_bill_date) then 'new' end
    ,visit
    ,prev_bill_date
    ,case when visit > 1
        then (case when bill_date - prev_bill_date > 365 then 'REPEAT_DORMANT' else 'REPEAT_ACTIVE' end)
        when visit = 1 and mob_mtched_lylty_id is not null
        then (case when enrollmentchannelcode = 'EYEPLUS' then 'NEW_ENCIRCLE' else 'NEW_CHANNEL' end)
        when visit = 1 and mob_mtched_lylty_id is null
        then 'NEW_ENCIRCLE'
        --when visit = 2
        --then (case when extract(month from bill_date) = extract(month from prev_bill_date) and extract(year from bill_date) = extract(year from prev_bill_date)
        --  then (case when enrollmentchannelcode = 'EYEPLUS' then 'NEW_ENCIRCLE' else 'NEW_CHANNEL' end) end)
        end as new_repeat_buyer_tag
    --,case when visit > 1
    --  then (case when bill_date - prev_bill_date > 365 then 'REPEAT_DORMANT' else 'REPEAT_ACTIVE' end)
    --  when visit = 1
    --  then (case when enrollmentchannelcode = 'EYEPLUS' then 'NEW_ENCIRCLE' else 'NEW_CHANNEL' end)
    --  end as new_repeat_bill_tag
    from (
select sales_cte.*
    ,visit_cte.visit
    ,visit_cte.prev_bill_date
    ,gld.enrollmentchannelcode
    from sales_cte
    left join visit_cte
        on sales_cte.cust_code = visit_cte.cust_code and sales_cte.bill_date = visit_cte.bill_date
    left join (select distinct ulpmembershipid, enrollmentchannelcode from ds_fld_digit_inboud_db.edw_base_data_bi.t_dim_psx_gld_edw_bi where curr_flg = 'Y') gld
        on sales_cte.mob_mtched_lylty_id = gld.ulpmembershipid);

--11. mv_ew_outsiderx_customers
create materialized view fielddigital_eyewear.mv_ew_outsiderx_customers
as
select
   store_master.storecode
   ,store_master.bill_date
   ,customer_number
   ,pr_examin_by
   ,abm_name
   ,abm_mailid
   ,ltl
   ,status
   ,storecity
   ,storeregion
   ,country
   ,channel
   ,divisionname
   ,open_date
   ,combo_status
   ,mall_status
   ,owner_type
   ,rbm_name
   ,rbm_mailid
   ,benchmark_group
  from
    (select distinct storecode,bill_date,abm_name,abm_mailid,ltl,status,storecity,storeregion,country,channel,divisionname,open_date,combo_status,mall_status,owner_type,rbm_name,rbm_mailid,benchmark_group
       from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg) store_master
  left join
   (select distinct
     a.storecode
     ,a.order_date as bill_date
     ,b.customer_number
     ,a.pr_examin_by
    from
     (select
        storecode
        ,vbeln
        ,pr_examin_by
        ,order_date
      from
        (select
           storecode
           ,upper(trim(vbeln)) as vbeln
           ,erdat as order_date
           ,pr_examin_by
           ,row_number() over(partition by storecode,upper(trim(vbeln)) order by erdat desc) as rn
          from (select
                   nvl(b.current_storecode,a.storecode) as storecode
                   ,vbeln
                   ,erdat
                   ,pr_examin_by
                 from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_srcstg_ew_isr_trns_soheader_bi a
                  left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) b
                    on a.storecode = b.old_storecode))
            where rn=1) a
        inner join
          (select
              nvl(q.current_storecode,p.store_code) as storecode
              ,upper(trim(sale_order_no)) as sale_order_no
              ,date(createddate) as order_date
              ,customer_number
              ,netprice
              ,sddocumenttype
              ,reason_for_rejections
             from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_stg_ew_isr_trns_saleorder_bi p
               left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) q
                 on p.store_code = q.old_storecode) b
         on  a.vbeln = b.sale_order_no
         and a.storecode = b.storecode
       where lower(a.pr_examin_by)='outside rx') outrx
        on store_master.storecode = outrx.storecode
        and store_master.bill_date = outrx.bill_date;

--12. mv_ew_booking_directsales_customers
create materialized view fielddigital_eyewear.mv_ew_booking_directsales_customers
as
select
   store_master.storecode
   ,store_master.bill_date
   ,customer_number
   ,abm_name
   ,abm_mailid
   ,ltl
   ,status
   ,storecity
   ,storeregion
   ,country
   ,channel
   ,divisionname
   ,open_date
   ,combo_status
   ,mall_status
   ,owner_type
   ,rbm_name
   ,rbm_mailid
   ,benchmark_group
  from
    (select distinct storecode,bill_date,abm_name,abm_mailid,ltl,status,storecity,storeregion,country,channel,divisionname,open_date,combo_status,mall_status,owner_type,rbm_name,rbm_mailid,benchmark_group
       from fielddigital_eyewear.mv_tgt_ew_isr_fct_sales_stg) store_master
   left join
    (select distinct
        storecode
        ,order_date as bill_date
        ,customer_number
       from
        (select
             nvl(q.current_storecode,p.store_code) as storecode
             ,sale_order_no
             ,date(createddate) as order_date
             ,customer_number
             ,netprice
             ,sddocumenttype
             ,reason_for_rejections
            from ds_fld_digit_inboud_db.etl_wrk_data_bi.t_stg_ew_isr_trns_saleorder_bi p
              left join (select distinct old_storecode, current_storecode from fielddigital_eyewear.t_tgt_fld_ew_storecode_change_mapping where rec_status_cd=1) q
                on p.store_code = q.old_storecode
             where upper(itemcategorycode)='TAN'
               and isnull(reason_for_rejections,'') in ('0','')) a
           left join ds_fld_digit_inboud_db.edw_base_data_bi.t_tgt_ew_isr_mstr_tvakt_salesdoctypemaster_bi b
             on a.sddocumenttype =b.auart
          where upper(b.SALETYPE) in ('DIRECT SALES','BOOKING SALES')) b_ds
     on store_master.storecode = b_ds.storecode
     and store_master.bill_date = b_ds.bill_date;

-----------------------------------------end of script------------------------------------------------