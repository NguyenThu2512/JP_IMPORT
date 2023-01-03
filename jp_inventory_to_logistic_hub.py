#%% nhập thư viện
import warnings
import pandas as pd
warnings.filterwarnings('ignore')
import time
import pygsheets

#%% Kết nối với file Logistic Hub và  Japan Import
gc = pygsheets.authorize(service_file=r'Data\potent-orbit-371709-b58aa2a0a71c.json')
#logistic_hub
sh_logistic_hub=gc.open_by_key('1UYvnalxJyQBPdkds5X8v-bZF0Jz4xHXOlP7uwHD79E0')
wks_logistic_hub=sh_logistic_hub.worksheet_by_title('logistic_hub')
#jp_import
sh_jp_inventory=gc.open_by_key('1GZ_lfj7XemTuXxGKMibKueo6wGfJ1wNv7oLMRP2BXTM')
wks_jp_export=sh_jp_inventory.worksheet_by_title('4. Export')

#%%
while True:
    # Đếm tổng số dòng hiện có trong sheet export
    jp_export_last_row = 0
    for x in wks_jp_export.get_col(1):
        if x != "":
            jp_export_last_row += 1
    jp_export_df = wks_jp_export.get_as_df(has_header=True, start=None, end=f'X{jp_export_last_row}')
    existed_product_id = wks_logistic_hub.get_as_df(has_header=False)[2]
    cond1 = (~jp_export_df['product_id'].isin(existed_product_id))
    cond2 = (~(jp_export_df['package_id'] == "")
             & ~(jp_export_df['lot_id'] == "")
             & ~(jp_export_df['partner_id'] == "")
             & ~(jp_export_df['jp_date_export'] == "")
             & ~(jp_export_df['jp_export_confirm'] == "FALSE"))
    true_logistic_data = jp_export_df.loc[cond1 & cond2].copy()
    true_row = true_logistic_data.shape[0]
    print("Đã lấy dữ liệu từ jp_export")
    if true_row == 0:
        print('Không có dữ liệu nào mới')
        continue
    #get last row of jp_export
    wks_jp_export_last_row_i = []
    lenth_row = len(wks_logistic_hub.get_row(1))
    for i in range(1, lenth_row):
        last_row = 1
        for x in wks_logistic_hub.get_col(i):
            if x != "":
                last_row = last_row + 1
        wks_jp_export_last_row_i.append(last_row)
    wks_jp_import_last_row_max = max(wks_jp_export_last_row_i)

    # Lấy hình ảnh từ jp_import
    product_image = []
    image_link = true_logistic_data['jp_product_image_link']
    for i in image_link:
        x = f'=IMAGE("{i}")'
        product_image.append(x)

    #  Thay đổi vị trí cột và thêm giá trị rỗng
    empty_lst = [""] * true_row
    need_column = [x for x in wks_logistic_hub.get_row(1) if x not in wks_jp_export.get_row(1)]
    for x in need_column:
        true_logistic_data[f'{x}'] = empty_lst
    true_logistic_data['product_image'] = product_image
    true_logistic_data = true_logistic_data[wks_logistic_hub.get_row(1)]
    # thêm dòng trước khi viết trong trường hợp trang hết dòng và thực hiện ghi
    print("Chuẩn bị ghi")
    wks_logistic_hub.add_rows(true_row)
    wks_logistic_hub.set_dataframe(true_logistic_data, start=f"A{wks_jp_import_last_row_max}", copy_head=False)
