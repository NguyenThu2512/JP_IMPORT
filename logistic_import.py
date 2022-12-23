#%% import library

import warnings

import pandas as pd
warnings.filterwarnings('ignore')
import time
import pygsheets
from IPython.display import Image, HTML
#%% import data

gc = pygsheets.authorize(service_file=r'Data\potent-orbit-371709-b58aa2a0a71c.json')
#logistic_hub
sh_logistic_hub=gc.open_by_key('1UYvnalxJyQBPdkds5X8v-bZF0Jz4xHXOlP7uwHD79E0')
wks_logistic_hub=sh_logistic_hub.worksheet_by_title('logistic_hub')
#jp_import
sh_jp_inventory=gc.open_by_key('1GZ_lfj7XemTuXxGKMibKueo6wGfJ1wNv7oLMRP2BXTM')
wks_jp_import=sh_jp_inventory.worksheet_by_title('jp_import')
#jp_export
wks_jp_export=sh_jp_inventory.worksheet_by_title('jp_export')
#vn_import
sh_vn_inventory=gc.open_by_key('1wW3ixtYOthX66jGrTpBqrnG5-MJWnfK1_h6u0tpTEl0')
wks_vn_import=sh_vn_inventory.worksheet_by_title('vn_import')
#%%
#Lấy dữ liệu ở các cột cần thiết trong jp_import
jp_import_df = wks_jp_import.get_as_df(has_header=True)
jp_import_df_copy = jp_import_df[['tracking_id','product_id', 'product_name','product_image',
                           'jp_date_received']].copy()

existed_product_id = wks_logistic_hub.get_as_df(has_header=False)[3]
true_logistic_data = jp_import_df_copy.loc[(~jp_import_df_copy['product_id'].isin(existed_product_id))].copy()

print("Đã lấy được dữ liệu từ jp_import")

#%% Thay đổi vị trí cột và thêm giá trị rỗng
empty_lst = [""] * true_logistic_data.shape[0]
true_logistic_data['international_ship_fee'] = empty_lst
true_logistic_data['payment_status']=empty_lst
true_logistic_data['package_id']=empty_lst
true_logistic_data['jp_date_export']=empty_lst
true_logistic_data['vn_date_import']=empty_lst
true_logistic_data['weight_rounded']=empty_lst
true_logistic_data = true_logistic_data[wks_logistic_hub.get_row(1)]

wks_logistic_hub_last_row = 1
for x in wks_logistic_hub.get_col(5):
    if x != "":
        wks_logistic_hub_last_row += 1
# thêm dòng trước khi viết trong trường hợp trang hết dòng
print("Chuẩn bị ghi")
wks_logistic_hub.add_rows(true_logistic_data.shape[0])

wks_logistic_hub.set_dataframe(true_logistic_data, start=f"A{wks_logistic_hub_last_row}", copy_head=False, extend=True)
logistic_hub_df=wks_logistic_hub.get_as_df(has_header=True)

#%% lấy dữ liệu ở jp_export

jp_export_df=wks_jp_export.get_as_df(has_header=True)
jp_export_df_copy=jp_export_df[['package_id','product_id','jp_date_export']].copy()
true_logistic_data = jp_export_df_copy.copy()
print("Đã lấy được dữ liệu từ jp_export")

#%% Thay vị trí cột và thêm vào giá trị null

empty_lst = [""] * true_logistic_data.shape[0]
true_logistic_data['tracking_id']=logistic_hub_df['tracking_id']
true_logistic_data['product_id']=logistic_hub_df['product_id'].astype(str)
true_logistic_data['product_name']=logistic_hub_df['product_name']
true_logistic_data['product_image']=logistic_hub_df['product_image']
true_logistic_data['jp_date_received']=logistic_hub_df['jp_date_received']
true_logistic_data['vn_date_import']=empty_lst
true_logistic_data['weight_rounded']=empty_lst
true_logistic_data['international_ship_fee'] = empty_lst
true_logistic_data['payment_status']=empty_lst
true_logistic_data = true_logistic_data[wks_logistic_hub.get_row(1)]
true_logistic_data_copy=true_logistic_data.copy()



#%%Lấy dữ liệu từ vn_import
vn_import_df = wks_vn_import.get_as_df(has_header=True)
vn_import_df_copy = vn_import_df[['product_id','vn_date_import','weight_rounded']].copy()

true_logistic_data = vn_import_df_copy.copy()
print("Đã lấy được dữ liệu từ jp_export")

#%% Thay đổi vị trí cột và thêm dữ liệu trống(phần này dữ liệu phần bên dưới xuất hiện null)

empty_lst = [""] * true_logistic_data.shape[0]
true_logistic_data['tracking_id']=logistic_hub_df['tracking_id']
true_logistic_data['package_id']=true_logistic_data_copy['package_id']
true_logistic_data['product_id']=logistic_hub_df['product_id'].astype(str)
true_logistic_data['product_name']=logistic_hub_df['product_name']
true_logistic_data['product_image']=logistic_hub_df['product_image']
true_logistic_data['jp_date_received']=logistic_hub_df['jp_date_received']
true_logistic_data['jp_date_export']=true_logistic_data_copy['jp_date_export']
true_logistic_data['international_ship_fee'] = empty_lst
true_logistic_data['payment_status']=empty_lst
true_logistic_data = true_logistic_data[wks_logistic_hub.get_row(1)]

#%% export data to logistic_hub
wks_logistic_hub_last_row = 1
for x in wks_logistic_hub.get_col(5):
    if x != "":
        wks_logistic_hub_last_row += 1
# thêm dòng trước khi viết trong trường hợp trang hết dòng
print("Chuẩn bị ghi")
wks_logistic_hub.set_dataframe(true_logistic_data, start=f"A{wks_logistic_hub_last_row}", copy_head=False, extend=True)
logistic_hub_df=wks_logistic_hub.get_as_df(has_header=True)

#%%
# logistic_hub_tong=pd.merge(jp_import_df_copy, jp_export_df_copy)
# logistic_hub_tong=pd.merge(logistic_hub_tong,vn_import_df_copy)
# df = jp_import_df_copy.merge(jp_export_df_copy).merge(vn_import_df_copy)


