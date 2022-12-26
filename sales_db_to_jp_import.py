#%% import library
import warnings
warnings.filterwarnings('ignore')
import time
import pygsheets

#%% import data from Sales Database
gc = pygsheets.authorize(service_file=r'Data\potent-orbit-371709-b58aa2a0a71c.json')

sh_sales_db = gc.open_by_key('1IBLGL-zLr_mtUUQllNmAZeLHMnutUzb34h35SeB6uAQ')
wks_sales_db = sh_sales_db.worksheet_by_title('Sales Database')

#%%get data from japan import
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
    print("Lấy dữ liệu lần đầu")
    true_row=true_logistic_data.shape[0]
    if true_row == 0:
        print('Không có dữ liệu được thêm mới')
        continue
    # get last row
    wks_jp_import_last_row_i = []
    lenth_row = len(wks_jp_import.get_row(1))
    for i in range(1, lenth_row):
        last_row = 1
        for x in wks_jp_import.get_col(i):
            if x != "":
                last_row = last_row + 1
        wks_jp_import_last_row_i.append(last_row)
    wks_jp_import_last_row_max = max(wks_jp_import_last_row_i)
    #Get image
    product_image = []
    for i in range(wks_jp_import_last_row_max, wks_jp_import_last_row_max + true_logistic_data.shape[0]):
        x = f'=IMAGE(I{i})'
        product_image.append(x)

    #Fill giá trị null vào những cột không được đổ
    empty_lst = [""] * true_row
    need_column = [x for x in wks_jp_import.get_row(1) if x not in wks_sales_db.get_row(1)]
    for x in need_column:
        true_logistic_data[f'{x}'] = empty_lst
    true_logistic_data['product_image_link'] = sales_db_df['product_image'].copy()
    true_logistic_data['product_image'] = product_image
    true_logistic_data = true_logistic_data[wks_jp_import.get_row(1)]

    # thêm dòng trước khi viết trong trường hợp trang hết dòng và thực hiện ghi file
    print("Chuẩn bị ghi")
    wks_jp_import.add_rows(true_logistic_data.shape[0])

    wks_jp_import.set_dataframe(true_logistic_data, start=f"A{wks_jp_import_last_row_max}", copy_head=False)

