#%% nhập thư viện
import warnings
import pandas as pd
warnings.filterwarnings('ignore')
import pygsheets

#%% Kết nối với file Japan Export
gc = pygsheets.authorize(service_file=r'Data\potent-orbit-371709-b58aa2a0a71c.json')
#jp_export
sh_jp_inventory=gc.open_by_key('1GZ_lfj7XemTuXxGKMibKueo6wGfJ1wNv7oLMRP2BXTM')
wks_jp_export=sh_jp_inventory.worksheet_by_title('4. Export')
#%%
while True:
    # Lấy những cột cần thiết
    jp_export_df=wks_jp_export.get_as_df(has_header=True)
    print("Đã lấy được dữ liệu từ jp_export")
    # Thêm cột check để kiểm tra tên có duplicate hay không
    jp_export_df['check']=jp_export_df['customer_name'].duplicated(keep=False)
    jp_export_df=jp_export_df.reset_index()
    #Lấy ra nhưng trường hợp cho package_id
    #Trường hợp gom kiện
    jp_export_df_package=jp_export_df.loc[(jp_export_df['package_need']=="TRUE")]
    df_name_package=jp_export_df_package['customer_name'].unique()
    df_name_package=pd.DataFrame(df_name_package,columns=['customer_name'])
    #Trường hợp không gom kiện
    jp_export_df_not_package=jp_export_df.loc[(jp_export_df['package_need']=="FALSE")]
    df_name_not_package=jp_export_df_not_package['customer_name'].reset_index()
    df_name_not_package=pd.DataFrame(df_name_not_package, columns=['customer_name'])
    #Tạo cột rỗng
    jp_export_df['package_id']=''*jp_export_df.shape[0]
    #Tạo package_id cho các trường hợp
    #package_id cho những người gom chung kiện hàng
    for i in range(df_name_package.shape[0]):
        for x in (jp_export_df_package['index']):
            if (jp_export_df['customer_name'][x]==df_name_package['customer_name'][i]):
                jp_export_df['package_id'][x]="PKG"+ str(i+1)
            else:
                continue

    #package_id cho những người không chung kiện hàng
    range_name=len(df_name_package)
    for z in range(df_name_not_package.shape[0]):
        for y in (jp_export_df_not_package['index']):
            if (jp_export_df['customer_name'][y]==df_name_not_package['customer_name'][z]):
                jp_export_df['package_id'][y]="PKG"+ str(z+1+range_name)
            else:
                continue
    # cập nhật package_id vào sheet Export
    print("Chuẩn bị cập nhật package_id")
    jp_export_df['index']=jp_export_df['index']+2
    for i in (jp_export_df['index']):
        wks_jp_export.update_value(f'T{i}',jp_export_df['package_id'][i-2])
    test=wks_jp_export.get_as_df(has_header=True)
