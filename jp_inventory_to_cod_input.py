#%% nhập thư viện
import warnings
import pandas as pd
warnings.filterwarnings('ignore')
import time
import pygsheets

#%% Kết nối với file Logistic Hub và  Japan Import
gc = pygsheets.authorize(service_file=r'Data\potent-orbit-371709-b58aa2a0a71c.json')
#cod_input
sh_cod_input=gc.open_by_key('1Atn4BWEd_Ci5iH3Dp9y01pdoBlZJboaGUtyKVho1FyQ')
wks_cod_input=sh_cod_input.worksheet_by_title('6. COD Input')
#jp_import
sh_jp_inventory=gc.open_by_key('1GZ_lfj7XemTuXxGKMibKueo6wGfJ1wNv7oLMRP2BXTM')
wks_jp_inventory=sh_jp_inventory.worksheet_by_title('3. Inventory')

#%%
while True:
    print("Đang lấy dữ liệu từ file jp_inventory")
    jp_inventory_df = wks_jp_inventory.get_as_df(has_header=True)
    existed_product_id = wks_cod_input.get_as_df(has_header=False)[2]
    cond1 = (~jp_inventory_df['product_id'].isin(existed_product_id))
    true_cod_data = jp_inventory_df.loc[(cond1)].copy()
    true_row = true_cod_data.shape[0]
    print("Đã lấy được dữ liệu từ jp_import")

    if true_row == 0:
        print('Không có dữ liệu nào mới')
        continue
    #get last row
    cod_input_last_row = 0
    for x in wks_cod_input.get_col(1):
        if x != "":
            cod_input_last_row += 1
    # Lấy hình ảnh từ jp_import
    product_image = []
    image_link = true_cod_data['product_image_link']
    for i in image_link:
        x = f'=IMAGE("{i}")'
        product_image.append(x)

    #   Thay đổi vị trí cột và thêm giá trị rỗng
    empty_lst = [""] * true_row
    need_column = [x for x in wks_cod_input.get_row(1) if x not in wks_jp_inventory.get_row(1)]
    for x in need_column:
        true_cod_data[f'{x}'] = empty_lst
    true_cod_data['fee_type'] = "COD"
    true_cod_data['product_image'] = product_image
    true_cod_data = true_cod_data[wks_cod_input.get_row(1)]


    # thêm dòng trước khi viết trong trường hợp trang hết dòng và thực hiện ghi
    print("Chuẩn bị ghi")
    wks_cod_input.add_rows(true_row)
    wks_cod_input.set_dataframe(true_cod_data, start=f"A{cod_input_last_row}", copy_head=False)
