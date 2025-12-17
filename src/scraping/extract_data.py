import pandas as pd
import json

def extract_scraping_data(file_path):
    """
    Extracts data from a JSON file and returns two pandas DataFrames.

    Parameters:
    file_path (str): The path to the JSON file.

    Returns:
    tuple: (products_df, reviews_df) - Two separate DataFrames for products and reviews
    """
    try:
        # Method 1: Try reading with json module first (more robust)
        print(f"📂 Đang đọc file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        print(f"✅ Đã đọc thành công {len(df)} sản phẩm")
        print(f"📊 Columns: {list(df.columns)}")
        
        # Chọn các columns cần thiết
        # Lưu ý: 'brand' là dict/object nên cần extract 'name' từ nó
        columns_to_keep = ['id', 'name', 'price', 'original_price', 'discount', 
                          'rating', 'quantity_sold', 'brand', 'specifications', 'stock_item']
        
        # Chỉ giữ các columns tồn tại trong dataframe
        available_columns = [col for col in columns_to_keep if col in df.columns]
        products_df = df[available_columns].copy()
        
        # Extract brand name nếu brand là dict
        if 'brand' in products_df.columns:
            products_df['brand_name'] = products_df['brand'].apply(
                lambda x: x.get('name', '') if isinstance(x, dict) else str(x) if x else ''
            )
            # Drop column brand gốc sau khi đã extract
            products_df = products_df.drop(columns=['brand'])
        
        # Tạo DataFrame riêng cho reviews
        reviews_df = None
        if 'reviews' in df.columns:
            print("🔄 Đang extract reviews...")
            reviews_data = []
            
            for _, row in df.iterrows():
                product_id = row['id']
                reviews = row.get('reviews', [])
                
                # Chỉ thêm nếu product có reviews
                if reviews and isinstance(reviews, list) and len(reviews) > 0:
                    for review in reviews:
                        if isinstance(review, dict):
                            review_data = {
                                'product_id': product_id,
                                'review_id': review.get('id'),
                                'title': review.get('title', ''),
                                'content': review.get('content', ''),
                                'rating': review.get('rating', 0),
                                'author': review.get('author', 'Anonymous'),
                                'time': review.get('time', ''),
                                'helpful_count': review.get('helpful_count', 0)
                            }
                            reviews_data.append(review_data)
            
            if reviews_data:
                reviews_df = pd.DataFrame(reviews_data)
                print(f"✅ Đã tạo reviews DataFrame với {len(reviews_df)} reviews")
            else:
                print("⚠️  Không tìm thấy reviews nào")
                reviews_df = pd.DataFrame(columns=['product_id', 'review_id', 'title', 'content', 
                                                   'rating', 'author', 'time', 'helpful_count'])
        
        return products_df, reviews_df
        
    except json.JSONDecodeError as e:
        print(f"❌ Lỗi JSON format: {e}")
        print(f"   Vị trí lỗi: line {e.lineno}, column {e.colno}")
        
        # Try alternative method: read line by line (for JSONL format)
        try:
            print("🔄 Thử đọc dạng JSON Lines (mỗi dòng 1 object)...")
            data = []
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:
                        try:
                            data.append(json.loads(line))
                        except json.JSONDecodeError:
                            print(f"⚠️  Skip dòng {line_num}: không parse được")
                            continue
            
            if data:
                df = pd.DataFrame(data)
                print(f"✅ Đã đọc thành công {len(df)} sản phẩm (JSONL format)")
                return df, None
            else:
                print("❌ Không có dữ liệu hợp lệ")
                return None, None
                
        except Exception as e2:
            print(f"❌ Lỗi khi đọc JSONL: {e2}")
            return None
    
    except Exception as e:
        print(f"❌ Lỗi khi đọc file: {e}")
        
        # Try pandas read_json with different parameters
        try:
            print("🔄 Thử với pandas.read_json và lines=True...")
            df = pd.read_json(file_path, lines=True)
            print(f"✅ Đã đọc thành công {len(df)} sản phẩm")
            return df, None
        except Exception as e3:
            print(f"❌ Pandas cũng không đọc được: {e3}")
            return None, None
    
if __name__ == "__main__":
    # Example usage
    file_path = "tiki_product.json"
    products_df, reviews_df = extract_scraping_data(file_path)
    
    if products_df is not None:
        print("\n" + "="*80)
        print(f"📊 THỐNG KÊ DỮ LIỆU PRODUCTS")
        print("="*80)
        print(f"Tổng số sản phẩm: {len(products_df)}")
        print(f"Tổng số cột: {len(products_df.columns)}")
        print(f"\n📋 Các cột trong products dataset:")
        for i, col in enumerate(products_df.columns, 1):
            print(f"   {i}. {col}")
        print("\n" + "="*80)
        print("🔍 XEM MỘT VÀI SẢN PHẨM ĐẦU TIÊN:")
        print("="*80)
        print(products_df.head())
        
        # Show info about data types
        print("\n" + "="*80)
        print("📈 THÔNG TIN CHI TIẾT PRODUCTS:")
        print("="*80)
        print(products_df.info())

        # Write products to csv
        output_csv = "extracted_products.csv"
        products_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"\n✅ Products đã được lưu vào file: {output_csv}")
    
    if reviews_df is not None and len(reviews_df) > 0:
        print("\n" + "="*80)
        print(f"📊 THỐNG KÊ DỮ LIỆU REVIEWS")
        print("="*80)
        print(f"Tổng số reviews: {len(reviews_df)}")
        print(f"Tổng số cột: {len(reviews_df.columns)}")
        print(f"\n📋 Các cột trong reviews dataset:")
        for i, col in enumerate(reviews_df.columns, 1):
            print(f"   {i}. {col}")
        print("\n" + "="*80)
        print("🔍 XEM MỘT VÀI REVIEWS ĐẦU TIÊN:")
        print("="*80)
        print(reviews_df.head())
        
        # Show info about data types
        print("\n" + "="*80)
        print("📈 THÔNG TIN CHI TIẾT REVIEWS:")
        print("="*80)
        print(reviews_df.info())
        
        # Write reviews to csv
        reviews_csv = "product_reviews.csv"
        reviews_df.to_csv(reviews_csv, index=False, encoding='utf-8-sig')
        print(f"\n✅ Reviews đã được lưu vào file: {reviews_csv}")
    else:
        print("\n⚠️  Không có reviews để lưu")