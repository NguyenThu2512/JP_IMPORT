#Source code này được tạo ra nhằm tự động đổ dữ liệu từ file genkin_jp_export đến file partner_jp_import sử dụng thư viện chính là pygsheets

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
#%% Sử dụng while True để chạy vòng lặp liên tục đổ dữ liệu ngay khi có sự thay đổi từ file gốc
while True:
    # Đếm tổng số dòng hiện có trong sheet export
    jp_export_last_row = 0 
    for x in wks_jp_export.get_col(1):
        if x != "":
            jp_export_last_row += 1
    
    #Lấy dữ liệu từ file gốc(genkin_jp_export) và chuyển sang định dạng dataframe
    print("Đang lấy dữ liệu từ file jp_export")
    jp_export_df = wks_jp_export.get_as_df(has_header=True, start=None, end=f'X{jp_export_last_row}')
    existed_product_id_import = wks_partner_import.get_as_df(has_header=False)[2]
    existed_product_id_inventory=wks_partner_inventory.get_as_df(has_header=False)[2]
    existed_product_id_export=wks_partner_export.get_as_df(has_header=False)[2]
    cond1 = (~jp_export_df['product_id'].isin(existed_product_id_import))    #Lấy điều kiện product_id không trùng với file đã được đổ từ file partner_import
    cond2 = (~jp_export_df['product_id'].isin(existed_product_id_inventory)) #Lấy điều kiện product_id không trùng với file đã được đổ từ file partner_inventory
    cond3=(~jp_export_df['product_id'].isin(existed_product_id_export))      #Lấy điều kiện product_id không trùng với file đã được đổ từ file partner_export
    cond4 = (~(jp_export_df['package_id'] == "")                             #Lấy điều kiện cột không rỗng
             & ~(jp_export_df['lot_id'] == "")
             & ~(jp_export_df['partner_id'] == "")
             & ~(jp_export_df['jp_date_export'] == "")
             & (jp_export_df['transport'] == "Air KKI")
             & (jp_export_df['jp_export_confirm'] == "TRUE"))
    true_partner_data = jp_export_df.loc[(cond1) & (cond2) & (cond3) & (cond4)].copy() # Dữ liệu đổ được lọc dữ liệu từ những điều kiện đã có 
    print("Đã lấy được dữ liệu từ jp_import")
    
    #Lấy số dòng hiện tại trong file được đổ(parter_jp_import) và kiểm tra số dòng
    true_row = true_partner_data.shape[0]
    if true_row == 0:
        print('Không có dữ liệu nào mới')
        continue
    # Lấy dòng cuối cùng của file parter_import (thỏa điều kiện toàn bộ dòng đều null)
    wks_partner_import_last_row_i = []
    lenth_row = len(wks_partner_import.get_row(1))
    for i in range(1, lenth_row):
        last_row = 1
        for x in wks_partner_import.get_col(i):
            if x != "":
                last_row = last_row + 1
        wks_partner_import_last_row_i.append(last_row)
    wks_partner_import_last_row_max = max(wks_partner_import_last_row_i)

    # lấy hình ảnh từ file gốc
    product_image = []
    image_link = true_partner_data['jp_product_image_link']
    for i in image_link:
        x = f'=IMAGE("{i}")'
        product_image.append(x)
    #Thay đổi vị trí cột vàthêm giá trị rỗng cho những cột không có trong file gốc và
    empty_lst = [""] * true_row        #Lấy giá trị rỗng với số hàng từ dữ liệu được đổ về(partner_import)
    need_column = [x for x in wks_partner_import.get_row(1) if x not in wks_jp_export.get_row(1)]  #Lấy những cột từ file được đổ(partner_import) mà không có trong file gốc
    for x in need_column:
        true_partner_data[f'{x}'] = empty_lst
    true_partner_data['jp_product_image'] = product_image 
    true_partner_data = true_partner_data[wks_partner_import.get_row(1)] #Dữ liệu cần đổ lấy cột của file partner_import

    #thực hiện ghi
    print("Chuẩn bị ghi")
    wks_partner_import.add_rows(true_row)    #thêm dòng trước khi viết trong trường hợp trang hết dòng và
    wks_partner_import.set_dataframe(true_partner_data, start=f"A{wks_partner_import_last_row_max}", copy_head=False)
