#%% nhập thư viện
import warnings
import pandas as pd
warnings.filterwarnings('ignore')
import time
import pygsheets

#%% Kết nối với file vn_import và  jp_export
gc = pygsheets.authorize(service_file=r'Data\potent-orbit-371709-b58aa2a0a71c.json')
#vn_import
sh_vn_inventory=gc.open_by_key('1wW3ixtYOthX66jGrTpBqrnG5-MJWnfK1_h6u0tpTEl0')
wks_vn_import=sh_vn_inventory.worksheet_by_title('vn_import')
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
    existed_product_id = wks_vn_import.get_as_df(has_header=False)[2]
    cond1=(~jp_export_df['product_id'].isin(existed_product_id))
    cond2 = (~(jp_export_df['package_id'] == "")
             & ~(jp_export_df['lot_id'] == "")
             & ~(jp_export_df['partner_id'] == "")
             & ~(jp_export_df['jp_date_export'] == "")
             & ~(jp_export_df['jp_export_confirm'] == "FALSE"))
    true_vn_data = jp_export_df.loc[(cond1)&(cond2)].copy()
    true_row=true_vn_data.shape[0]

    if true_row == 0:
        print('Không có dữ liệu nào mới')
        continue
    print("Đã lấy được dữ liệu từ jp_import")
    #get last row of vn_import
    wks_vn_import_last_row_i = []
    lenth_row = len(wks_vn_import.get_row(1))
    for i in range(1, lenth_row):
        last_row = 1
        for x in wks_vn_import.get_col(i):
            if x != "":
                last_row = last_row + 1
        wks_vn_import_last_row_i.append(last_row)
    wks_jp_import_last_row_max = max(wks_vn_import_last_row_i)

    #  Thay đổi vị trí cột và thêm giá trị rỗng
    empty_lst = [""] * true_row
    true_vn_data['genkin_weight'] = pd.to_numeric(true_vn_data['genkin_weight'], downcast="float")
    true_vn_data['genkin_weight'] = true_vn_data['genkin_weight'].apply(lambda x: "%.3f" % round(x, 3))
    row_genkin_weight = true_vn_data['genkin_weight']
    need_column = [x for x in wks_vn_import.get_row(1) if x not in wks_jp_export.get_row(1)]
    for x in need_column:
        true_vn_data[f'{x}'] = empty_lst
    true_vn_data['weight_rounded'] = row_genkin_weight
    true_vn_data = true_vn_data[wks_vn_import.get_row(1)]

    # thêm dòng trước khi viết trong trường hợp trang hết dòng và thực hiện ghi
    print("Chuẩn bị ghi")
    wks_vn_import.add_rows(true_row)
    wks_vn_import.set_dataframe(true_vn_data, start=f"A{wks_jp_import_last_row_max}", copy_head=False)
