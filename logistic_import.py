#%% nhập thư viện
import warnings
import pandas as pd
warnings.filterwarnings('ignore')
import time
import pygsheets
from IPython.display import Image, HTML

#%% Kết nối với file Logistic Hub và  Japan Import
gc = pygsheets.authorize(service_file=r'Data\potent-orbit-371709-b58aa2a0a71c.json')
#logistic_hub
sh_logistic_hub=gc.open_by_key('1UYvnalxJyQBPdkds5X8v-bZF0Jz4xHXOlP7uwHD79E0')
wks_logistic_hub=sh_logistic_hub.worksheet_by_title('logistic_hub')
#jp_import
sh_jp_inventory=gc.open_by_key('1GZ_lfj7XemTuXxGKMibKueo6wGfJ1wNv7oLMRP2BXTM')
wks_jp_import=sh_jp_inventory.worksheet_by_title('jp_import')

while True:
    try:
        print("Đang lấy dữ liệu từ file jp_import")
        jp_import_df = wks_jp_import.get_as_df(has_header=True)
        jp_import_df_copy = jp_import_df[['tracking_id', 'product_id', 'product_name', 'product_image_link',
                                          'jp_date_received']].copy()

        existed_product_id = wks_logistic_hub.get_as_df(has_header=False)[3]
        true_logistic_data = jp_import_df_copy.loc[(~jp_import_df_copy['product_id'].isin(existed_product_id))].copy()

        print("Đã lấy được dữ liệu từ jp_import lần 1")
        print("Lấy order lần 1")

        if true_logistic_data.shape[0] == 0:
            print('Không có dữ liệu nào mới')
            nm + 1
        else:
            print("Chờ 5s lỡ đâu tích nhầm")
            time.sleep(5)
            jp_import_df = wks_jp_import.get_as_df(has_header=True)
            jp_import_df_copy = jp_import_df[['tracking_id', 'product_id', 'product_name', 'product_image_link',
                                              'jp_date_received']].copy()
            existed_product_id = wks_logistic_hub.get_as_df(has_header=False)[3]
            true_logistic_data = jp_import_df_copy.loc[
                (~jp_import_df_copy['product_id'].isin(existed_product_id))].copy()
        print("Đã lấy được dữ liệu từ jp_import")
        # Lấy hình ảnh tư jp_import
        product_image = []
        image_link = true_logistic_data['product_image_link']
        for i in image_link:
            x = f'=IMAGE("{i}")'
            product_image.append(x)
        #  Thay đổi vị trí cột và thêm giá trị rỗng
        empty_lst = [""] * true_logistic_data.shape[0]
        true_logistic_data['product_image'] = product_image
        true_logistic_data['international_ship_fee'] = empty_lst
        true_logistic_data['payment_status'] = empty_lst
        true_logistic_data['package_id'] = empty_lst
        true_logistic_data['jp_date_export'] = empty_lst
        true_logistic_data['vn_date_import'] = empty_lst
        true_logistic_data['weight_rounded'] = empty_lst
        true_logistic_data = true_logistic_data[wks_logistic_hub.get_row(1)]
        wks_logistic_hub_last_row = 1
        for x in wks_logistic_hub.get_col(5):
            if x != "":
                wks_logistic_hub_last_row += 1
        # thêm dòng trước khi viết trong trường hợp trang hết dòng
        print("Chuẩn bị ghi")
        wks_logistic_hub.add_rows(true_logistic_data.shape[0])

        wks_logistic_hub.set_dataframe(true_logistic_data, start=f"A{wks_logistic_hub_last_row}", copy_head=False,
                                       extend=True)
        logistic_hub_df = wks_logistic_hub.get_as_df(has_header=True)

    except NameError:
        print("Đang lấy dữ liệu từ file jp_import")
        jp_import_df = wks_jp_import.get_as_df(has_header=True)
        jp_import_df_copy = jp_import_df[['tracking_id', 'product_id', 'product_name', 'product_image_link',
                                          'jp_date_received']].copy()

        existed_product_id = wks_logistic_hub.get_as_df(has_header=False)[3]
        true_logistic_data = jp_import_df_copy.loc[(~jp_import_df_copy['product_id'].isin(existed_product_id))].copy()

        print("Đã lấy được dữ liệu từ jp_import lần 1")
        print("Lấy order lần 1")

        if true_logistic_data.shape[0] == 0:
            print('Không có dữ liệu nào mới')
            continue
        else:
            print("Chờ 5s lỡ đâu tích nhầm")
            time.sleep(5)
            jp_import_df = wks_jp_import.get_as_df(has_header=True)
            jp_import_df_copy = jp_import_df[['tracking_id', 'product_id', 'product_name', 'product_image_link',
                                              'jp_date_received']].copy()
            existed_product_id = wks_logistic_hub.get_as_df(has_header=False)[3]
            true_logistic_data = jp_import_df_copy.loc[
                (~jp_import_df_copy['product_id'].isin(existed_product_id))].copy()
        print("Đã lấy được dữ liệu từ jp_import")
        # Lấy hình ảnh tư jp_import
        product_image = []
        image_link = true_logistic_data['product_image_link']
        for i in image_link:
            x = f'=IMAGE("{i}")'
            product_image.append(x)
        #  Thay đổi vị trí cột và thêm giá trị rỗng
        empty_lst = [""] * true_logistic_data.shape[0]
        true_logistic_data['product_image'] = product_image
        true_logistic_data['international_ship_fee'] = empty_lst
        true_logistic_data['payment_status'] = empty_lst
        true_logistic_data['package_id'] = empty_lst
        true_logistic_data['jp_date_export'] = empty_lst
        true_logistic_data['vn_date_import'] = empty_lst
        true_logistic_data['weight_rounded'] = empty_lst
        true_logistic_data = true_logistic_data[wks_logistic_hub.get_row(1)]
        wks_logistic_hub_last_row = 1
        for x in wks_logistic_hub.get_col(5):
            if x != "":
                wks_logistic_hub_last_row += 1
        # thêm dòng trước khi viết trong trường hợp trang hết dòng
        print("Chuẩn bị ghi")
        wks_logistic_hub.add_rows(true_logistic_data.shape[0])

        wks_logistic_hub.set_dataframe(true_logistic_data, start=f"A{wks_logistic_hub_last_row}", copy_head=False,
                                       extend=True)
        logistic_hub_df = wks_logistic_hub.get_as_df(has_header=True)
    finally:
        time.sleep(5)