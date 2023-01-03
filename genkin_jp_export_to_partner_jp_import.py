#%% nhập thư viện
import warnings
import pandas as pd
warnings.filterwarnings('ignore')
import time
import pygsheets

#%% Kết nối với file partner_jp_import và  jp_export
gc = pygsheets.authorize(service_file=r'Data\potent-orbit-371709-b58aa2a0a71c.json')
#partner_jp_import
sh_partner_inventory=gc.open_by_key('11Jl2q9rKSVIiiSEJe8eQkRefCzu_Bu9pAEkTmob4RK0')
wks_partner_import=sh_partner_inventory.worksheet_by_title('2. Partner Import')
wks_partner_inventory=sh_partner_inventory.worksheet_by_title('3. Partner Inventory')
wks_partner_export=sh_partner_inventory.worksheet_by_title('4. Partner Export')
#jp_export
sh_jp_inventory=gc.open_by_key('1GZ_lfj7XemTuXxGKMibKueo6wGfJ1wNv7oLMRP2BXTM')
wks_jp_export=sh_jp_inventory.worksheet_by_title('4. Export')
#%%
while True:
    # Đếm tổng số dòng hiện có trong sheet export
    jp_export_last_row = 0
    for x in wks_jp_export.get_col(1):
        if x != "":
            jp_export_last_row += 1
    print("Đang lấy dữ liệu từ file jp_export")
    jp_export_df = wks_jp_export.get_as_df(has_header=True, start=None, end=f'X{jp_export_last_row}')
    existed_product_id_import = wks_partner_import.get_as_df(has_header=False)[2]
    existed_product_id_inventory=wks_partner_inventory.get_as_df(has_header=False)[2]
    existed_product_id_export=wks_partner_export.get_as_df(has_header=False)[2]
    cond1 = (~jp_export_df['product_id'].isin(existed_product_id_import))
    cond2 = (~jp_export_df['product_id'].isin(existed_product_id_inventory))
    cond3=(~jp_export_df['product_id'].isin(existed_product_id_export))
    cond4 = (~(jp_export_df['package_id'] == "")
             & ~(jp_export_df['lot_id'] == "")
             & ~(jp_export_df['partner_id'] == "")
             & ~(jp_export_df['jp_date_export'] == "")
             & (jp_export_df['transport'] == "Air KKI")
             & (jp_export_df['jp_export_confirm'] == "TRUE"))
    true_partner_data = jp_export_df.loc[(cond1) & (cond2) & (cond3) & (cond4)].copy()
    true_row = true_partner_data.shape[0]
    print("Đã lấy được dữ liệu từ jp_import")
    if true_row == 0:
        print('Không có dữ liệu nào mới')
        continue
    # get last row of worksheet parter_import
    wks_partner_import_last_row_i = []
    lenth_row = len(wks_partner_import.get_row(1))
    for i in range(1, lenth_row):
        last_row = 1
        for x in wks_partner_import.get_col(i):
            if x != "":
                last_row = last_row + 1
        wks_partner_import_last_row_i.append(last_row)
    wks_partner_import_last_row_max = max(wks_partner_import_last_row_i)

    #  Thay đổi vị trí cột và thêm giá trị rỗng
    # Get image
    product_image = []
    image_link = true_partner_data['jp_product_image_link']
    for i in image_link:
        x = f'=IMAGE("{i}")'
        product_image.append(x)
    #
    empty_lst = [""] * true_row
    need_column = [x for x in wks_partner_import.get_row(1) if x not in wks_jp_export.get_row(1)]
    for x in need_column:
        true_partner_data[f'{x}'] = empty_lst
    true_partner_data['jp_product_image'] = product_image
    true_partner_data = true_partner_data[wks_partner_import.get_row(1)]

    # thêm dòng trước khi viết trong trường hợp trang hết dòng và thực hiện ghi
    print("Chuẩn bị ghi")
    wks_partner_import.add_rows(true_row)
    wks_partner_import.set_dataframe(true_partner_data, start=f"A{wks_partner_import_last_row_max}", copy_head=False)
