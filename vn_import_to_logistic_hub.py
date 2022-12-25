#%% nhập thư viện
import warnings
warnings.filterwarnings('ignore')
import time
import pygsheets

#%% kết nối dữ liệu từ file logistic_hub và file vn_import
gc = pygsheets.authorize(service_file=r'Data\potent-orbit-371709-b58aa2a0a71c.json')
#logistic_hub
sh_logistic_hub=gc.open_by_key('1UYvnalxJyQBPdkds5X8v-bZF0Jz4xHXOlP7uwHD79E0')
wks_logistic_hub=sh_logistic_hub.worksheet_by_title('logistic_hub')
#vn_import
sh_vn_inventory=gc.open_by_key('1wW3ixtYOthX66jGrTpBqrnG5-MJWnfK1_h6u0tpTEl0')
wks_vn_import=sh_vn_inventory.worksheet_by_title('vn_import')

#%%Tự động cập nhật dữ liệu từ file vn_import
while True:
    try:
        print("Đang cập nhật dữ liệu từ file vn_import")
        # lấy dữ liệu ở file vn_import
        vn_import_df = wks_vn_import.get_as_df(has_header=True)
        #Lấy những cột cần lấy
        true_logistic_data = vn_import_df[['product_id','vn_date_import','weight_rounded']].copy()
        print("Đã lấy được dữ liệu từ vn_import")
        if(true_logistic_data.shape[0]==0):
            print("Chưa có dữ liệu mới từ vn_import để cập nhật")
            mn+1
        else:
            vn_import_df = wks_vn_import.get_as_df(has_header=True)
            # Lấy những cột cần lấy
            true_logistic_data = vn_import_df[['product_id', 'vn_date_import', 'weight_rounded']].copy()
            print("Đã lấy được dữ liệu từ vn_import")
            #Lọc dữ liệu ở logistic_hub có product_id khớp với vn_import và chỉ lấy những giá trị chưa được cập nhật
            product_id_vn_import=true_logistic_data['product_id']
            logistic_hub_df=wks_logistic_hub.get_as_df(has_header=True)
            cond1 = (product_id_vn_import.isin(logistic_hub_df['product_id']))
            cond2 = ((logistic_hub_df['vn_date_import'] == '') & (logistic_hub_df['weight_rounded'] == ''))
            logistic_hub_df = logistic_hub_df.loc[cond1 | cond2].reset_index().copy()
            #Cập nhật dữ liệu ở 2 cột vn_date_import và weight_rounded
            logistic_hub_df['index']=logistic_hub_df['index']+2
            for i in (logistic_hub_df['index']):
                wks_logistic_hub.update_value(f'H{i}',true_logistic_data['vn_date_import'][i-2])
                wks_logistic_hub.update_value(f'I{i}', true_logistic_data['weight_rounded'][i - 2])
    except NameError:
        print("Đang cập nhật dữ liệu từ file vn_import")
        # lấy dữ liệu ở file vn_import
        vn_import_df = wks_vn_import.get_as_df(has_header=True)
        # Lấy những cột cần lấy
        true_logistic_data = vn_import_df[['product_id', 'vn_date_import', 'weight_rounded']].copy()
        print("Đã lấy được dữ liệu từ vn_import")
        if (true_logistic_data.shape[0] == 0):
            print("Chưa có dữ liệu mới từ vn_import để cập nhật")
            continue;
        else:
            vn_import_df = wks_vn_import.get_as_df(has_header=True)
            # Lấy những cột cần lấy
            true_logistic_data = vn_import_df[['product_id', 'vn_date_import', 'weight_rounded']].copy()
            print("Đã lấy được dữ liệu từ vn_import")
            # Lọc dữ liệu ở logistic_hub có product_id khớp với vn_import và những dữ liệu chưa được cập nhật
            product_id_vn_import = true_logistic_data['product_id']
            logistic_hub_df = wks_logistic_hub.get_as_df(has_header=True)
            cond1 = (product_id_vn_import.isin(logistic_hub_df['product_id']))
            cond2 = ((logistic_hub_df['vn_date_import'] == '') & (logistic_hub_df['weight_rounded'] == ''))
            logistic_hub_df = logistic_hub_df.loc[cond1 | cond2].reset_index().copy()

            logistic_hub_df['index'] = logistic_hub_df['index'] + 2
            for i in (logistic_hub_df['index']):
                wks_logistic_hub.update_value(f'H{i}', true_logistic_data['vn_date_import'][i - 2])
                wks_logistic_hub.update_value(f'I{i}', true_logistic_data['weight_rounded'][i - 2])
    finally:
        time.sleep(5)





