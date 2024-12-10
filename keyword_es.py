import pymysql
import requests
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
from datetime import datetime
from index_setting import index_settings

# .env 파일 로드
load_dotenv()

# MySQL 설정
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")

# Elasticsearch 설정
ES_HOST = os.getenv("ES_HOST")
ES_PORT = os.getenv("ES_PORT")

# 키워드 추출 API 설정
KEYWORD_API_URL = os.getenv("API_URL")  # AI 서버 API URL

#MySQL 데이터 가져오기
def fetch_data_from_mysql():
    today = datetime.now().strftime('%Y-%m-%d')
    connection = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4"
    )
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT post_id, post_title, post_content, created_at
                FROM post
                WHERE DATE(created_at) = %s 
            """
            #오늘 업데이트 한 내용
            cursor.execute(query, (today,))
            data = cursor.fetchall()
        return data
    finally:
        connection.close()
# def fetch_data_from_mysql():
#     return [
#         (21, "21번째 게시글 제목", "오늘 날씨가 매우 맑고 쾌적합니다.", "2024-12-10 10:00:00"),
#         (22, "22번째 게시글 제목", "최근 개봉한 영화 중 최고는 무엇일까요?", "2024-12-10 10:30:00"),
#         (23, "23번째 게시글 제목", "헬스장에서 효과적으로 운동하는 방법에 대해 알려주세요.", "2024-12-10 11:00:00"),
#         (24, "24번째 게시글 제목", "최근 읽은 책 리뷰와 추천 부탁드립니다.", "2024-12-10 11:30:00"),
#         (25, "25번째 게시글 제목", "Python과 JavaScript의 차이점에 대해 이야기합니다.", "2024-12-10 12:00:00")
#     ]

# Elasticsearch에 데이터 저장
def save_to_elasticsearch(es_client, index_name, postid, keywords):
    for keyword in keywords:
        print(f"저장 중: post_id={postid}, keyword={keyword}")
        doc = {
            "post_id": postid,
            "keyword": keyword
        }
        es_client.index(index=index_name, document=doc)


# 키워드 추출 요청
def extract_keywords_from_api(title, content):
    try:
        response = requests.post(
            KEYWORD_API_URL,
            json={"title": title, "content": content}
        )
        response.raise_for_status()
        result = response.json()
        print(f"API 응답 데이터: {result}")  # 디버깅용
        # 키워드 리스트 반환
        return result.get("frequency_keywords", [])
    except requests.RequestException as e:
        print(f"키워드 추출 API 요청 중 오류 발생: {e}")
        return []
    
# 메인 실행 함수
def main():
    print("시작")
    data = fetch_data_from_mysql()
    print(f"MySQL 데이터는 : {data}")
    
    es_port = int(ES_PORT)
    es = Elasticsearch([{"host": ES_HOST, "port": es_port, "scheme": "http"}])
    print(f"엘라스틱서치 시작")
    
    index_name = "keywords"
    
    try:
        if es.indices.exists(index=index_name):
            print(f"인덱스 '{index_name}'가 이미 존재합니다.")
        else:
            
            es.indices.create(index=index_name, body=index_settings)
            print(f"인덱스 '{index_name}'가 생성되었습니다.")
    except Exception as e:
        print(f"인덱스 생성 중 오류 발생: {e}")
    
    for row in data:
        postid, post_title, post_content, created_at = row
        text = f"{post_title} {post_content}"
        
        # 키워드 추출 API 호출
        frequency_keywords = extract_keywords_from_api(post_title, post_content)
        print(f"추출된 빈도 기반 키워드: {frequency_keywords}")
        
        # Elasticsearch에 저장
        save_to_elasticsearch(es, index_name, postid, frequency_keywords)
    
    print("데이터 처리가 완료되었습니다.")

if __name__ == "__main__":
    main()
