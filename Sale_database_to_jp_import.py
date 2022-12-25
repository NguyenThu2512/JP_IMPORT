#%% nhập thư viện%Việt test abcd
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
    try:
        print("Đang lấy dữ liệu từ sales database")
        # Chỗ này em chưa hiểu nè chị
        sales_db_df = wks_sales_db.get_as_df(has_header=False)
        sales_db_df.columns = wks_sales_db.get_row(1)
        sales_db_df = sales_db_df[['product_id', 'date', 'phone_number', 'transport',
                                   'product_name', 'product_link', 'product_image',
                                   'exchange_rate', 'purchase_fee']].copy()
        # Feedback2
        existed_product_id = wks_jp_import.get_as_df(has_header=False)[1]
        true_sales_data = sales_db_df.loc[(~sales_db_df['product_id'].isin(existed_product_id))].copy()
        print("Lấy dữ liệu lần đầu")

        if true_sales_data.shape[0] == 0:
            print('Không có dữ liệu được thêm mới')
            nm + 1
        else:
            print("Chờ 5s trong trường hợp tích nhầm")
            time.sleep(5)
            sales_db_df = wks_sales_db.get_as_df(has_header=False)
            sales_db_df.columns = wks_sales_db.get_row(1)
            sales_db_df = sales_db_df[['product_id', 'date', 'phone_number', 'transport',
                                       'product_name', 'product_link', 'product_image',
                                       'exchange_rate', 'purchase_fee']].copy()
            existed_product_id = wks_jp_import.get_as_df(has_header=False)[1]
            true_sales_data = sales_db_df.loc[(~sales_db_df['product_id'].isin(existed_product_id))].copy()

        print("Đã lấy được dữ liệu từ Sales_DB")

        #Get image
        sales_db_df_image = sales_db_df['product_image'].copy()
        true_sales_data['product_image_link'] = sales_db_df_image
        product_image = []
        wks_jp_import_last_row = 1
        for x in wks_jp_import.get_col(3):
            if x != "":
                wks_jp_import_last_row += 1
        for i in range(wks_jp_import_last_row, wks_jp_import_last_row + true_sales_data.shape[0]):
            x = f'=IMAGE(I{i})'
            product_image.append(x)
        #Thay đổi thứ tự cột và bổ sụng giá trị trống cho những cột k nằm trong file nguồn
        empty_lst = [""] * true_sales_data.shape[0]
        true_sales_data['tracking_id'] = empty_lst
        true_sales_data['genkin_weight'] = empty_lst
        true_sales_data['product_image'] = product_image
        true_sales_data['jp_date_received'] = empty_lst
        true_sales_data['jp_product_image_link'] = empty_lst
        true_sales_data['jp_cod'] = empty_lst
        true_sales_data['purchase_fee'] = empty_lst
        true_sales_data['jp_received_confirm'] = empty_lst
        true_sales_data = true_sales_data[wks_jp_import.get_row(1)]

        # ghi dữ liệu lên file jp_import
        wks_jp_import_last_row = 1
        for x in wks_jp_import.get_col(3):
            if x != "":
                wks_jp_import_last_row += 1
        # thêm dòng trước khi viết trong trường hợp trang hết dòng và ghi dữ liệu
        print("Chuẩn bị ghi")
        wks_jp_import.add_rows(true_sales_data.shape[0])
        wks_jp_import.set_dataframe(true_sales_data, start=f"A{wks_jp_import_last_row}", copy_head=False, extend=True)
    except NameError:
        print("Đang lấy dữ liệu từ sales database")
        sales_db_df = wks_sales_db.get_as_df(has_header=False)
        sales_db_df.columns = wks_sales_db.get_row(1)
        sales_db_df = sales_db_df[['product_id', 'date', 'phone_number', 'transport',
                                   'product_name', 'product_link', 'product_image',
                                   'exchange_rate', 'purchase_fee']].copy()
        existed_product_id = wks_jp_import.get_as_df(has_header=False)[1]
        true_sales_data = sales_db_df.loc[(~sales_db_df['product_id'].isin(existed_product_id))].copy()
        print("Lấy dữ liệu lần đầu")
        if true_sales_data.shape[0] == 0:
            print('Không có dữ liệu nào được thêm mới')
            continue
        else:
            print("Chờ 5s trong trường hợp tích nhầm")
            time.sleep(5)
            sales_db_df = wks_sales_db.get_as_df(has_header=False)
            sales_db_df.columns = wks_sales_db.get_row(1)
            sales_db_df = sales_db_df[['product_id', 'date', 'phone_number', 'transport',
                                       'product_name', 'product_link', 'product_image',
                                       'exchange_rate', 'purchase_fee']].copy()
            existed_product_id = wks_jp_import.get_as_df(has_header=False)[1]
            true_sales_data = sales_db_df.loc[(~sales_db_df['product_id'].isin(existed_product_id))].copy()

        print("Đã lấy được dữ liệu từ Sales_DB")

        # Lấy hình ảnh
        sales_db_df_image = sales_db_df['product_image'].copy()
        true_sales_data['product_image_link'] = sales_db_df_image
        product_image = []
        wks_jp_import_last_row = 1
        for x in wks_jp_import.get_col(5):
            if x != "":
                wks_jp_import_last_row += 1
        for i in range(wks_jp_import_last_row, wks_jp_import_last_row + true_sales_data.shape[0]):
            x = f'=IMAGE(I{i})'
            product_image.append(x)
        #Thay đổi vị trí cột và thêm giá trị rỗng
        empty_lst = [""] * true_sales_data.shape[0]
        true_sales_data['tracking_id'] = empty_lst
        true_sales_data['genkin_weight'] = empty_lst
        true_sales_data['product_image'] = product_image
        true_sales_data['jp_date_received'] = empty_lst
        true_sales_data['jp_product_image_link'] = empty_lst
        true_sales_data['jp_cod'] = empty_lst
        true_sales_data['purchase_fee'] = empty_lst
        true_sales_data['jp_received_confirm'] = empty_lst
        true_sales_data = true_sales_data[wks_jp_import.get_row(1)]
        # reindex columns
        # true_sales_data = true_sales_data.reindex(
        #     columns=['tracking_id', 'product_id', 'date', 'phone_number', 'transport',
        #              'product_name', 'genkin_weight', 'product_link', 'product_image_link', 'product_image',
        #              'jp_date_received', 'jp_product_image_link', 'jp_cod', 'exchange_rate', 'purchase_fee'])

        #ghi dữ liệu lên file jp_import
        wks_jp_import_last_row = 1
        for x in wks_jp_import.get_col(5):
            if x != "":
                wks_jp_import_last_row += 1
        # thêm dòng trước khi viết trong trường hợp trang hết dòng
        print("Chuẩn bị ghi")
        wks_jp_import.add_rows(true_sales_data.shape[0])
        wks_jp_import.set_dataframe(true_sales_data, start=f"A{wks_jp_import_last_row}", copy_head=False, extend=True)
    finally:
        time.sleep(5)