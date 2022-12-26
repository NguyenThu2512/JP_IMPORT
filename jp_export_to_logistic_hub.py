#%% nhập thư viện
import warnings
warnings.filterwarnings('ignore')
import time
import pygsheets
#%% kết nối với file logistic_hub và file jp_export
gc = pygsheets.authorize(service_file=r'Data\potent-orbit-371709-b58aa2a0a71c.json')
#logistic_hub
sh_logistic_hub=gc.open_by_key('1UYvnalxJyQBPdkds5X8v-bZF0Jz4xHXOlP7uwHD79E0')
wks_logistic_hub=sh_logistic_hub.worksheet_by_title('logistic_hub')
#jp_inventory - jp_export
sh_jp_inventory=gc.open_by_key('1GZ_lfj7XemTuXxGKMibKueo6wGfJ1wNv7oLMRP2BXTM')
wks_jp_export=sh_jp_inventory.worksheet_by_title('jp_export')

#%% Tự động cập nhật dữ liệu
while True:
    print("Đang cập nhật jp_export")
    # lấy dữ liệu ở jp_export
    jp_export_df=wks_jp_export.get_as_df(has_header=True)

    cond1 = (~(jp_export_df['tracking_id'] == "")
             & ~(jp_export_df['product_id'] == "")
             & ~(jp_export_df['package_id'] == "")
             & ~(jp_export_df['lot_id'] == "")
             & ~(jp_export_df['partner_id'] == "")
             & ~(jp_export_df['jp_date_export'] == "")
             & ~(jp_export_df['jp_export_confirm'] == "FALSE"))
    true_logistic_data = jp_export_df.loc[(cond1)].copy()

    print("Đã lấy được dữ liệu từ jp_export ")

    if(true_logistic_data.shape[0]==0):
        print("Chưa có dữ liệu từ jp_export")
        continue
    else:
        #Lấy những cột cần thiết ở jp_export
        jp_export_df = wks_jp_export.get_as_df(has_header=True)
        true_logistic_data = jp_export_df.loc[(cond1)].copy()
        print("Đã lấy được dữ liệu từ jp_export ")

        #Lọc dữ liệu có product_id giống nhau ở logistic_hub_df
        product_id_export=true_logistic_data['product_id']
        logistic_hub_df=wks_logistic_hub.get_as_df(has_header=True)
        cond1=(product_id_export.isin(logistic_hub_df['product_id']))
        cond2=((logistic_hub_df['package_id']=='') & (logistic_hub_df['jp_date_export']==''))
        logistic_hub_df=logistic_hub_df.loc[cond1|cond2].reset_index().copy()
        #Gắn index đối xừng với bảng sheet
        logistic_hub_df['index']=logistic_hub_df['index']+2
        #Cập nhật dữ liệu package_id và jp_date_export từ jp_export
        for i in (logistic_hub_df['index']):
            wks_logistic_hub.update_value(f'B{i}',true_logistic_data['package_id'][i-2])
            wks_logistic_hub.update_value(f'G{i}', true_logistic_data['jp_date_export'][i - 2])



