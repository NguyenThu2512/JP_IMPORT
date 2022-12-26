#%% nhập thư viện
import warnings
warnings.filterwarnings('ignore')
import time
import pygsheets
#%% Kết nối dữ liệu từ google sheet
gc = pygsheets.authorize(service_file=r'Data\potent-orbit-371709-b58aa2a0a71c.json')
#Kết nối dữ liệu sales database
sh_sales_db = gc.open_by_key('1IBLGL-zLr_mtUUQllNmAZeLHMnutUzb34h35SeB6uAQ')
wks_sales_db = sh_sales_db.worksheet_by_title('Sales Database')

#%%kết nối file jp_import
sh_jp_import=gc.open_by_key('1GZ_lfj7XemTuXxGKMibKueo6wGfJ1wNv7oLMRP2BXTM')
wks_jp_import=sh_jp_import.worksheet_by_title('jp_import')
#%%
while True:
    print("Đang lấy dữ liệu từ sales database")
    sales_db_df = wks_sales_db.get_as_df(has_header=True)
    sales_db_df = sales_db_df[['product_id', 'date', 'phone_number', 'transport',
                               'product_name', 'product_link', 'product_image','jp_cod',
                               'exchange_rate', 'purchase_fee']]
    existed_product_id = wks_jp_import.get_as_df(has_header=False)[1]
    true_logistic_data = sales_db_df.loc[(~sales_db_df['product_id'].isin(existed_product_id))].copy()
    true_row = true_logistic_data.shape[0]
    print("Lấy dữ liệu lần đầu")
    if true_logistic_data.shape[0] == 0:
        print("Không có dữ liệu mới")
        continue
    # Lấy row cuối cùng trong sheet
    wks_jp_import_last_row_i = []
    lenth_row = len(wks_jp_import.get_row(1))
    for i in range(1, lenth_row):
        last_row = 1
        for x in wks_jp_import.get_col(i):
            if x != "":
                last_row = last_row + 1
        wks_jp_import_last_row_i.append(last_row)
    wks_jp_import_last_row_max = max(wks_jp_import_last_row_i)

    #Lấy hình ảnh
    true_logistic_data['product_image_link'] = sales_db_df['product_image'].copy()
    product_image = []
    wks_jp_import_last_row = 1
    for i in range(wks_jp_import_last_row_max, wks_jp_import_last_row_max + true_row):
        x = f'=IMAGE(I{i})'
        product_image.append(x)

    #Thay đổi vị trí cột và thêm giá trị rỗng
    empty_lst = [""] * true_logistic_data.shape[0]
    need_column=[x for x in wks_jp_import.get_row(1) if x not in wks_sales_db.get_row(1)]
    for x in need_column:
        true_logistic_data[f'{x}'] = empty_lst
    sales_db_df_image = sales_db_df['product_image'].copy()
    true_logistic_data['product_image_link'] = sales_db_df_image
    true_logistic_data['product_image'] = product_image
    true_logistic_data = true_logistic_data[wks_jp_import.get_row(1)]

    # ghi dữ liệu lên file jp_import
    # thêm dòng trước khi viết trong trường hợp trang hết dòng và ghi dữ liệu
    print("Chuẩn bị ghi")
    wks_jp_import.add_rows(true_row)
    wks_jp_import.set_dataframe(true_logistic_data, start=f"A{wks_jp_import_last_row_max}", copy_head=False, extend=True)
