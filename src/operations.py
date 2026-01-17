import re
import psycopg2
import numpy as np
from langchain_openai import OpenAIEmbeddings
from sklearn.metrics.pairwise import cosine_similarity

# ========== Connecting and disconnecting to database ========================

def connect_to_db():
    conn_params = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "your_password_here",
        "host": "localhost",
        "port": "5432"
    }
    return psycopg2.connect(**conn_params)

def disconnect(cur, conn):
    conn.commit()
    cur.close()
    conn.close()
    return

# Enabling LangChain OpenAI API
OPENAI_API_KEY = 'YOUR_OPENAI_API_KEY_HERE'
embeddings_model = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY, model="text-embedding-ada-002")

# ========== Setting up database  ============================================

def drop_existing_tables():
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("DROP VIEW IF EXISTS weekly_ranking")
    cur.execute("DROP VIEW IF EXISTS monthly_ranking")
    cur.execute("DROP TABLE IF EXISTS ratings")
    cur.execute("DROP TABLE IF EXISTS users")
    cur.execute("DROP TABLE IF EXISTS movies")
    disconnect(cur, conn)

def create_tables():
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
        uid SERIAL PRIMARY KEY,
        uname VARCHAR(50) NOT NULL,
        gender VARCHAR(1) NOT NULL,
        age INTEGER NOT NULL); """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movies (
        mid SERIAL PRIMARY KEY,
        title VARCHAR(50) NOT NULL,
        director VARCHAR(50) NOT NULL,
        nationality VARCHAR(50) NOT NULL,
        release_year INTEGER NOT NULL,
        profit INTEGER NOT NULL,
        summary VARCHAR(500) NOT NULL,
        embedding FLOAT8[] NOT NULL);""")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ratings (
        uid INTEGER NOT NULL,
        mid INTEGER NOT NULL,
        rating REAL NOT NULL,
        time DATE NOT NULL,
        PRIMARY KEY (uid, mid),
        FOREIGN KEY (uid) REFERENCES users(uid) ON DELETE CASCADE,
        FOREIGN KEY (mid) REFERENCES movies(mid) ON DELETE CASCADE);""")
    disconnect(cur, conn)

def add_data_from_txt():
    conn = connect_to_db()
    cur = conn.cursor()
    f = open("codes/data/users.txt", "r")
    for line in f:
        data = line.split(';')
        uname = data[0]
        gender = data[1]
        age = data[2]
        insert_user(uname, gender, age)
    f.close()
    f = open("codes/data/movies.txt", "r")
    for line in f:
        line = line.rstrip('\n')
        data = line.split(';')
        title = data[0]
        director = data[1]
        nationality = data[2]
        release_year = data[3]
        profit = re.sub(',', '', data[4])
        summary = data[5]
        insert_movie(title, director, nationality, release_year, profit, summary)
    f.close()
    f = open("codes/data/ratings.txt", "r")
    for line in f:
        data = line.split(';')
        uid = data[0]
        mid = data[1]
        rating = data[2]
        time = data[3]
        insert_rating(uid, mid, rating, time)
    f.close()
    disconnect(cur, conn)



# ========== Conventional Scenarios ==========================================

def insert_user(uname, gender, age):									# Keyboard Input을 통해 받은 User 정보를 이용하여 새로운 User를 추가하는 함수
    conn = connect_to_db()											# PostgreSQL에 연결
    cur = conn.cursor()												# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    query = """
        INSERT INTO users (uname, gender, age)
        VALUES (%s, %s, %s)
        RETURNING *
    """														# 입력 값을 이용하여 새로운 User를 추가 가능한 SQL 쿼리문 생성
    cur.execute(query, (uname, gender, age))									# SQL 쿼리 실행
    ret = cur.fetchone()											# 추가한 행 가져오기
    disconnect(cur, conn)											# PostgreSQL에 연결 종료
    return ret													# 추가된 User 정보 리턴

def insert_movie(title, director, nationality, release_year, profit, summary):				# Keyboard Input을 통해 받은 Movie 정보를 이용하여 새로운 Movie를 추가하는 함수
    conn = connect_to_db()											# PostgreSQL에 연결
    cur = conn.cursor()												# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    embedding = embeddings_model.embed_query(summary)								# OpenAI 임베딩 모델을 사용하여 Movie 요약 정보에 대한 임베딩 벡터 생성

    query = """
        INSERT INTO movies (title, director, nationality, release_year, profit, summary, embedding)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """														# 입력 값을 이용하여 새로운 Movie를 추가 가능한 SQL 쿼리문 생성
    cur.execute(query, (title, director, nationality, release_year, profit, summary, embedding))		# SQL 쿼리 실행
    ret = cur.fetchone()[:-1]											# 추가한 행 가져오기
    disconnect(cur, conn)											# PostgreSQL에 연결 종료
    return ret													# 추가된 Movie 정보 리턴

def insert_rating(uid, mid, rating, time):								# Keyboard Input을 통해 받은 Rating 정보를 이용하여 새로운 Rating를 추가하는 함수
    conn = connect_to_db()											# PostgreSQL에 연결
    cur = conn.cursor()												# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    query = """
        INSERT INTO ratings (uid, mid, rating, time)
        VALUES (%s, %s, %s, %s)
        RETURNING *
    """														# 입력 값을 이용하여 새로운 Rating를 추가 가능한 SQL 쿼리문 생성
    cur.execute(query, (uid, mid, rating, time))								# SQL 쿼리 실행
    ret = cur.fetchone()											# 추가한 행 가져오기
    disconnect(cur, conn)											# PostgreSQL에 연결 종료
    return ret													# 추가된 Rating 정보 리턴

def update_user(uid, uname=None, gender=None, age=None)									# Keyboard Input을 통해 받은 User 정보를 이용하여 User 정보를 갱신하는 함수
    conn = connect_to_db()													# PostgreSQL에 연결
    cur = conn.cursor()														# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    updates = []														# 갱신할 칼럼과 값을 저장할 리스트
    params = []															# SQL 쿼리에 사용할 파라미터 값 리스트
    if uname:															# 입력받은 정보에 이름이 있다면
        updates.append("uname = %s")													# 갱신할 칼럼 목록에 "uname = %s" 추가
        params.append(uname)														# 파라미터 리스트에 uname 추가
    if gender:															# 입력받은 정보에 성별이 있다면
        updates.append("gender = %s")													# 갱신할 칼럼 목록에 "gender = %s" 추가
        params.append(gender)														# 파라미터 리스트에 gender 추가
    if age:															# 입력받은 정보에 나이가 있다면
        updates.append("age = %s")													# 갱신할 칼럼 목록에 "age = %s" 추가
        params.append(age)														# 파라미터 리스트에 age 추가
    params.append(uid)														# 파라미터 리스트에 uid 추가
    query = "UPDATE users SET " + ", ".join(updates) + " WHERE uid = %s" + " RETURNING *"					# 입력 값을 이용하여 User 정보를 갱신하는 SQL 쿼리문 생성	
    cur.execute(query, tuple(params))												# SQL 쿼리 실행
    ret = cur.fetchone()													# 갱신된 행 가져오기
    disconnect(cur, conn)													# PostgreSQL에 연결 종료
    return ret															# 갱신된 User 정보 리턴

def update_movie(mid, title=None, director=None, nationality=None, release_year=None, profit=None, summary=None):	# Keyboard Input을 통해 받은 Movie 정보를 이용하여 Movie 정보를 갱신하는 함수
    conn = connect_to_db()													# PostgreSQL에 연결
    cur = conn.cursor()														# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    updates = []														# 갱신할 칼럼과 값을 저장할 리스트
    params = []															# SQL 쿼리에 사용할 파라미터 값 리스트
    if title:															# 입력받은 정보에 제목이 있다면
        updates.append("title = %s")													# 갱신할 칼럼 목록에 "title = %s" 추가
        params.append(title)														# 파라미터 리스트에 title 추가
    if director:														# 입력받은 정보에 감독이 있다면
        updates.append("director = %s")													# 갱신할 칼럼 목록에 "director = %s" 추가
        params.append(director)														# 파라미터 리스트에 director 추가
    if nationality:														# 입력받은 정보에 국가가 있다면
        updates.append("nationality = %s")												# 갱신할 칼럼 목록에 "nationality = %s" 추가
        params.append(nationality)													# 파라미터 리스트에 nationality 추가
    if release_year:														# 입력받은 정보에 연도가 있다면
        updates.append("release_year = %s")												# 갱신할 칼럼 목록에 "release_year = %s" 추가
        params.append(release_year)													# 파라미터 리스트에 release_year 추가
    if profit:															# 입력받은 정보에 수익이 있다면	
        updates.append("profit = %s")													# 갱신할 칼럼 목록에 "profit = %s" 추가
        params.append(profit)														# 파라미터 리스트에 profit 추가
    if summary:															# 입력받은 정보에 요약이 있다면
        updates.append("summary = %s")													# 갱신할 칼럼 목록에 "summary = %s" 추가
        params.append(summary)														# 파라미터 리스트에 summary 추가
        embedding = embeddings_model.embed_query(summary)										# OpenAI 임베딩 모델을 사용하여 Movie 요약 정보에 대한 임베딩 벡터 생성
        updates.append("embedding = %s")												# 갱신할 칼럼 목록에 "embedding = %s" 추가
        params.append(embedding)													# 파라미터 리스트에 embedding 추가
    params.append(mid)														# 파라미터 리스트에 mid 추가
    query = "UPDATE movies SET " + ", ".join(updates) + " WHERE mid = %s" + " RETURNING *"					# 입력 값을 이용하여 Movie 정보를 갱신하는 SQL 쿼리문 생성  
    cur.execute(query, tuple(params))												# SQL 쿼리 실행
    ret = cur.fetchone()[:-1]													# 갱신된 행 가져오기
    disconnect(cur, conn)													# PostgreSQL에 연결 종료
    return ret															# 갱신된 Movie 정보 리턴

def update_rating(uid, mid, rating=None, time=None):									# Keyboard Input을 통해 받은 Rating 정보를 이용하여 Rating 정보를 갱신하는 함수
    conn = connect_to_db()													# PostgreSQL에 연결
    cur = conn.cursor()														# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    updates = []														# 갱신할 칼럼과 값을 저장할 리스트
    params = []															# SQL 쿼리에 사용할 파라미터 값 리스트
    if rating:															# 입력받은 정보에 평점이 있다면
        updates.append("rating = %s")													# 갱신할 칼럼 목록에 "rating = %s" 추가
        params.append(rating)														# 파라미터 리스트에 rating 추가
    if time:															# 입력받은 정보에 날짜가 있다면
        updates.append("time = %s")													# 갱신할 칼럼 목록에 "time = %s" 추가
        params.append(time)														# 파라미터 리스트에 time 추가
    params.append(uid)														# 파라미터 리스트에 uid 추가
    params.append(mid)														# 파라미터 리스트에 mid 추가
    query = "UPDATE ratings SET " + ", ".join(updates) + " WHERE uid = %s AND mid = %s" + " RETURNING *"			# 입력 값을 이용하여 Rating 정보를 갱신하는 SQL 쿼리문 생성
    cur.execute(query, tuple(params))												# SQL 쿼리 실행
    ret = cur.fetchone()													# 갱신된 행 가져오기
    disconnect(cur, conn)													# PostgreSQL에 연결 종료
    return ret															# 갱신된 Rating 정보 리턴

def delete_user(uid):									# Keyboard Input을 통해 받은 User ID 정보를 이용하여 해당 User 정보를 삭제하는 함수
    conn = connect_to_db()									# PostgreSQL에 연결
    cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    cur.execute("DELETE FROM users WHERE uid = %s", (uid,))					# 입력 값을 이용하여 User 정보를 삭제하는 SQL 쿼리문 생성
    disconnect(cur, conn)									# PostgreSQL에 연결 종료

def delete_movie(mid):									# Keyboard Input을 통해 받은 Movie ID 정보를 이용하여 해당 Movie 정보를 삭제하는 함수
    conn = connect_to_db()									# PostgreSQL에 연결
    cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    cur.execute("DELETE FROM movies WHERE mid = %s", (mid,))					# 입력 값을 이용하여 Movie 정보를 삭제하는 SQL 쿼리문 생성
    disconnect(cur, conn)									# PostgreSQL에 연결 종료

def delete_rating(uid, mid):								# Keyboard Input을 통해 받은 User ID와 Movie ID 정보를 이용하여 관련 Rating 정보를 삭제하는 함수
    conn = connect_to_db()									# PostgreSQL에 연결
    cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    cur.execute("DELETE FROM ratings WHERE uid = %s AND mid = %s", (uid, mid))			# 입력 값을 이용하여 Rating 정보를 삭제하는 SQL 쿼리문 생성
    disconnect(cur, conn)									# PostgreSQL에 연결 종료

def fetch_user(uid): 									# Keyboard Input을 통해 받은 User ID (uid)에 일치하는 User 정보를 조회하는 함수
    conn = connect_to_db()									# PostgreSQL에 연결
    cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    cur.execute("SELECT * FROM users WHERE uid = %s", (uid,))					# SQL 쿼리 실행: users 테이블에서 uid가 일치하는 User의 모든 정보를 조회
    result = cur.fetchone()									# 조회된 행 가져오기
    disconnect(cur, conn)									# PostgreSQL에 연결 종료
    return result										# 조회된 User 정보 리턴

def fetch_movie(mid): 									# Keyboard Input을 통해 받은 Movie ID (mid)에 일치하는 Movie 정보를 조회하는 함수
    conn = connect_to_db()									# PostgreSQL에 연결
    cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    cur.execute("""										
        SELECT mid, title, director, nationality, release_year, profit, summary
        FROM movies 
        WHERE mid = %s
        """, (mid,))										# SQL 쿼리 실행: movies 테이블에서 mid가 일치하는 Movie의 모든 정보를 조회
    result = cur.fetchone()									# 조회된 행 가져오기
    disconnect(cur, conn)									# PostgreSQL에 연결 종료
    return result										# 조회된 Movie 정보 리턴

def fetch_rating(uid, mid): 								# Keyboard Input을 통해 받은 User ID (uid)와 Movie ID (mid)에 일치하는 Rating 정보를 조회하는 함수
    conn = connect_to_db()									# PostgreSQL에 연결
    cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    cur.execute("SELECT * FROM ratings WHERE uid = %s AND mid = %s", (uid, mid))		# SQL 쿼리 실행: ratings 테이블에서 uid와 mid가 일치하는 Rating의 모든 정보를 조회
    result = cur.fetchone()									# 조회된 행 가져오기
    disconnect(cur, conn)									# PostgreSQL에 연결 종료
    return result										# 조회된 Rating 정보 리턴

def show(table_name): 									# Keyboard Input을 통해 받은 테이블에 해당하는 모든 데이터를 조회하는 함수
    if (table_name in ["users", "ratings"]):							# users 혹은 ratings를 테이블 이름으로 받은 경우
        conn = connect_to_db()										# PostgreSQL에 연결
        cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
        str = "SELECT * FROM " + table_name + ";"							# 구분자 ;를 이용하여 테이블의 모든 데이터를 조회 가능한 SQL 쿼리문 생성
        cur.execute(str)										# SQL 쿼리 실행
        results = cur.fetchall()									# 조회된 모든 행 가져오기
        disconnect(cur, conn)										# PostgreSQL에 연결 종료		
        return results											# 조회된 정보 리턴
    if (table_name == "movies"):								# movies를 테이블 이름으로 받은 경우
        conn = connect_to_db()										# PostgreSQL에 연결
        cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
        cur.execute("""
            SELECT mid, title, director, nationality, release_year, profit, summary
            FROM movies 
            """)											# SQL 쿼리 실행: movies 테이블에서 embedding 필드를 제외한 모든 데이터 조회
        results = cur.fetchall()                                                                        # 조회된 모든 행 가져오기
        disconnect(cur, conn)										# PostgreSQL에 연결 종료
        return results											# 조회된 정보 리턴
    else: return None										# 테이블 이름이 users/movies/ratings 중에 없는 경우 None 리턴

def display_user_history(uid):								# Keyboard Input을 통해 받은 User ID (uid)에 일치하는 User 정보와 Movie 조회 이력을 나타내는 함수
    conn = connect_to_db()									# PostgreSQL에 연결
    cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    result = []											# 결과 저장할 리스트
    query1 = """
        SELECT *
        FROM users
        WHERE uid = %s
    """												# User 정보를 조회 가능한 SQL 쿼리문 생성
    cur.execute(query1, (uid,))									# SQL 쿼리 실행
    result.append(cur.fetchone())								# 조회된 행 가져오기
    query2 = """
        SELECT m.mid, m.title, r.rating, r.time
        FROM ratings r
        JOIN users u ON r.uid = u.uid
        JOIN movies m ON r.mid = m.mid
        WHERE u.uid = %s
        ORDER BY r.time DESC;
    """												# Movie 조회 이력을 탐색 가능한 SQL 쿼리문 생성
    cur.execute(query2, (uid,))									# SQL 쿼리 실행
    result.append(cur.fetchall())								# 조회된 모든 행 가져오기
    disconnect(cur, conn)									# PostgreSQL에 연결 종료
    return result										# 조회된 정보 리턴

def display_movie_ratings(mid): 							# Keyboard Input을 통해 받은 Movie ID (mid)에 일치하는 Movie 정보와 User가 Rating한 이력을 나타내는 함수
    conn = connect_to_db()									# PostgreSQL에 연결
    cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    result = []											# 결과 저장할 리스트
    query1 = """
        SELECT mid, title, director, nationality, release_year, profit, summary
        FROM movies
        WHERE mid = %s
    """												# Movie 정보를 조회 가능한 SQL 쿼리문 생성
    cur.execute(query1, (mid,))									# SQL 쿼리 실행
    result.append(cur.fetchone())								# 조회된 행 가져오기
    query2 = """
        SELECT r.uid, u.uname, r.rating, r.time
        FROM ratings r
        JOIN movies m ON r.mid = m.mid
        JOIN users u ON r.uid = u.uid
        WHERE m.mid = %s
        ORDER BY r.time DESC;
    """												# User가 Rating한 이력을 조회 가능한 SQL 쿼리문 생성
    cur.execute(query2, (mid,))									# SQL 쿼리 실행
    result.append(cur.fetchall())								# 조회된 모든 행 가져오기
    disconnect(cur, conn)									# PostgreSQL에 연결 종료
    return result										# 조회된 정보 리턴

def count(table_name): # count all rows in a relation when given the relation's name
    if (table_name in ["users", "movies", "ratings"]) :
        conn = connect_to_db()
        cur = conn.cursor()
        str = "SELECT COUNT(*) from " + table_name + ";"
        cur.execute(str)
        num = cur.fetchone()[0]
        disconnect(cur, conn)
        return num
    else: 0



# ========== Main Scenarios ==================================================

# ------------------------------ Scenario #1: Weekly/Monthly Ranking of Movies

def create_triggers(cur, type):									# Keyboard Input을 통해 받은 주간 랭킹 요청을 수행하기 위한 트리거를 갱신하는 함수
    
    if(type == "month"):								
        create_function_monthly = """									
        CREATE OR REPLACE FUNCTION refresh_monthly_ranking() RETURNS TRIGGER AS $$			# create_function_monthly 정의
        BEGIN
            EXECUTE '												# 월간 순위 뷰를 갱신하는 SQL 쿼리문 생성
                CREATE OR REPLACE VIEW monthly_ranking AS								# 월간 순위 뷰 생성 혹은 갱신
                SELECT m.mid, m.title, ROUND(AVG(r.rating::numeric), 2) AS avg_rating					# movie ID, movie title, 평균 평점 정보를 이용
                FROM movies m
                LEFT JOIN ratings r ON m.mid = r.mid AND r.time >= (CURRENT_DATE - 30)					# 테이블에 저장된 평점 날짜를 이용하여 최근 30일 평점만 고려
                GROUP BY m.mid, m.title
                ORDER BY avg_rating DESC NULLS LAST;									# 평균 평점은 내림차순 정렬로 나타내고 평점 없는 영화는 마지막으로 배치
            ';
            RETURN NULL;											# 트리거 함수는 결과를 반환하지 않음
        END;
        $$ LANGUAGE plpgsql;										# PostgreSQL을 이용
        """
        cur.execute(create_function_monthly)								# SQL 쿼리 실행
        
        cur.execute("DROP TRIGGER IF EXISTS refresh_monthly_ranking_trigger_movies ON movies;")		# 기존 트리거 삭제
        create_trigger_movies_monthly = """
            CREATE TRIGGER refresh_monthly_ranking_trigger_movies						# 트리거 refresh_monthly_ranking_trigger_movies 생성
            AFTER INSERT OR UPDATE OR DELETE ON movies								# INSERT, UPDATE, DELETE 발생 시 트리거 실행
            FOR EACH STATEMENT											# 각 행 실행 후에 트리거 실행
            EXECUTE FUNCTION refresh_monthly_ranking();								# refresh_monthly_ranking 함수 호출
        """
        cur.execute(create_trigger_movies_monthly)							# SQL 쿼리 실행

        cur.execute("DROP TRIGGER IF EXISTS refresh_monthly_ranking_trigger_ratings ON ratings;") 	# 기존 트리거 삭제
        create_trigger_ratings_monthly = """									
            CREATE TRIGGER refresh_monthly_ranking_trigger_ratings						# 트리거 refresh_monthly_ranking_trigger_ratings 생성
            AFTER INSERT OR UPDATE OR DELETE ON ratings								# INSERT, UPDATE, DELETE 발생 시 트리거 실행
            FOR EACH STATEMENT											# 각 행 실행 후에 트리거 실행
            EXECUTE FUNCTION refresh_monthly_ranking();								# refresh_monthly_ranking 함수 호출
        """
        cur.execute(create_trigger_ratings_monthly)							# SQL 쿼리 실행

    elif(type == "week"):
        create_function_weekly = """
        CREATE OR REPLACE FUNCTION refresh_weekly_ranking() RETURNS TRIGGER AS $$			# create_function_weekly 정의
        BEGIN
            EXECUTE '												# 주간 순위 뷰를 갱신하는 SQL 쿼리문 생성
                CREATE OR REPLACE VIEW weekly_ranking AS								# 주간 순위 뷰 생성 혹은 갱신
                SELECT m.mid, m.title, ROUND(AVG(r.rating)::numeric, 2) AS avg_rating					# movie ID, movie title, 평균 평점 정보를 이용
                FROM movies m								
                LEFT JOIN ratings r ON m.mid = r.mid AND r.time >= (CURRENT_DATE - 7)					# 테이블에 저장된 평점 날짜를 이용하여 최근 7일 평점만 고려
                GROUP BY m.mid, m.title
                ORDER BY avg_rating DESC NULLS LAST;									# 평균 평점은 내림차순 정렬로 나타내고 평점 없는 영화는 마지막으로 배치
            ';
            RETURN NULL;											# 트리거 함수는 결과를 반환하지 않음
        END;
        $$ LANGUAGE plpgsql;										# PostgreSQL을 이용
        """
        cur.execute(create_function_weekly)								# SQL 쿼리 실행
        
        cur.execute("DROP TRIGGER IF EXISTS refresh_weekly_ranking_trigger_movies ON movies;")		# 기존 트리거 삭제
        create_trigger_movies_weekly = """							
            CREATE TRIGGER refresh_weekly_ranking_trigger_movies						# 트리거 refresh_weekly_ranking_trigger_ratings 생성
            AFTER INSERT OR UPDATE OR DELETE ON movies								# INSERT, UPDATE, DELETE 발생 시 트리거 실행
            FOR EACH STATEMENT											# 각 행 실행 후에 트리거 실행
            EXECUTE FUNCTION refresh_weekly_ranking();								# refresh_weekly_ranking 함수 호출
        """
        cur.execute(create_trigger_movies_weekly)							# SQL 쿼리 실행

        cur.execute("DROP TRIGGER IF EXISTS refresh_weekly_ranking_trigger_ratings ON ratings;")	# 기존 트리거 삭제
        create_trigger_ratings_weekly = """								
            CREATE TRIGGER refresh_weekly_ranking_trigger_ratings						# 트리거 refresh_weekly_ranking_trigger_ratings 생성
            AFTER INSERT OR UPDATE OR DELETE ON ratings								# INSERT, UPDATE, DELETE 발생 시 트리거 실행
            FOR EACH STATEMENT											# 각 행 실행 후에 트리거 실행
            EXECUTE FUNCTION refresh_weekly_ranking();								# refresh_weekly_ranking 함수 호출
        """
        cur.execute(create_trigger_ratings_weekly)							# SQL 쿼리 실행

def show_monthly_ranking():										# Keyboard Input을 통해 받은 주간 랭킹 요청을 나타내는 함수
    conn = connect_to_db()											# PostgreSQL에 연결
    cur = conn.cursor()												# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    cur.execute("""
        CREATE OR REPLACE VIEW monthly_ranking AS							
        SELECT m.mid, m.title, ROUND(AVG(r.rating)::numeric, 2) AS avg_rating
        FROM movies m
        LEFT JOIN ratings r ON m.mid = r.mid AND r.time >= (CURRENT_DATE - 30)
        GROUP BY m.mid, m.title
        ORDER BY avg_rating DESC NULLS LAST;
    """)													# 트리거 갱신을 위한 함수인 create_function_monthly의 조건과 동일
    create_triggers(cur, "month")										# 트리거 갱신을 위한 함수 호출
    cur.execute("SELECT * FROM monthly_ranking;")								# SQL 쿼리 실행	
    results = cur.fetchall()											# 조회된 모든 행 가져오기
    disconnect(cur, conn)											# PostgreSQL에 연결 종료
    return results												# 조회된 정보 리턴


def show_weekly_ranking():										# Keyboard Input을 통해 받은 월간 랭킹 요청을 나타내는 함수
    conn = connect_to_db()											# PostgreSQL에 연결
    cur = conn.cursor()												# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    cur.execute("""
        CREATE OR REPLACE VIEW weekly_ranking AS
        SELECT m.mid, m.title, ROUND(AVG(r.rating)::numeric, 2) AS avg_rating
        FROM movies m
        LEFT JOIN ratings r ON m.mid = r.mid AND r.time >= (CURRENT_DATE - 7)
        GROUP BY m.mid, m.title
        ORDER BY avg_rating DESC NULLS LAST;
    """)													# 트리거 갱신을 위한 함수인 create_function_weekly의 조건과 동일
    create_triggers(cur, "week")										# 트리거 갱신을 위한 함수 호출
    cur.execute("SELECT * FROM weekly_ranking;")								# SQL 쿼리 실행
    results = cur.fetchall()											# 조회된 모든 행 가져오기
    disconnect(cur, conn)											# PostgreSQL에 연결 종료
    return results												# 조회된 정보 리턴

# ------------- Scenario #2: Content-based Recommendation for Cold-start Users

def get_cold_start():									# Keyboard Input을 통해 받은 Cold-start User 조회 요청을 수행하기 위한 함수
    conn = connect_to_db()									# PostgreSQL에 연결
    cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성
    query = """											# Cold-start User 조회를 위한 SQL 쿼리문 생성 이때 Rating이 하나도 없는 User는 고려하지 않는다
        SELECT u.uid, u.uname, u.gender, u.age, COUNT(*) as count
        FROM ratings r										
        JOIN users u ON r.uid = u.uid
        GROUP BY u.uid, u.uname, u.gender, u.age
        HAVING COUNT(*) < 2										# 평점 개수가 2개 미만인 사용자 만을 추출
        ORDER BY count;
        """
    cur.execute(query)										# SQL 쿼리 실행
    result = cur.fetchall()									# 조회된 모든 행 가져오기
    disconnect(cur, conn)									# PostgreSQL에 연결 종료
    return result										# 조회된 정보 리턴

def recommend_contents_based(uid):							# Keyboard Input을 통해 받은 contents-based 추천 요청을 수행하기 위한 함수
    conn = connect_to_db()									# PostgreSQL에 연결
    cur = conn.cursor()										# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성

    query1 = """									
        SELECT m.embedding
        FROM ratings r
        JOIN users u ON r.uid = u.uid
        JOIN movies m ON r.mid = m.mid
        WHERE u.uid = %s
        ORDER BY r.mid """
    cur.execute(query1, (uid,))									# SQL 쿼리 실행: User가 Rating한 영화들의 embedding 벡터 추출
    array1 = np.array(cur.fetchall()).squeeze(axis=1)						# array1: User가 Rating한 영화들의 embedding 벡터들을 numpy array로 저장

    query2 = """
        WITH rated_movies AS (
            SELECT r.mid
            FROM ratings r
            JOIN users u ON r.uid = u.uid
            WHERE u.uid = %s
            ORDER BY r.mid
        ) 
        SELECT m.embedding
        FROM movies m
        WHERE m.mid NOT IN (SELECT mid FROM rated_movies)
        ORDER BY m.mid
        """
    cur.execute(query2, (uid,))									# SQL 쿼리 실행: User가 Rating하지 않은 영화들의 embedding 벡터 추출
    array2 = np.array(cur.fetchall()).squeeze(axis=1)						# array2: User가 Rating하지 않은 영화들의 embedding 벡터들을 numpy array로 저장
    num_unrated = array2.shape[0]								# User가 Rating하지 않은 영화들의 개수 저장

    query3 = """
        SELECT r.rating
        FROM ratings r
        JOIN users u ON r.uid = u.uid
        WHERE u.uid = %s 
        ORDER BY r.mid """
    cur.execute(query3, (uid,))									# SQL 쿼리 실행: User가 Rating한 영화들의 평점을 추출
    array3 = np.array(cur.fetchall()).squeeze(axis=1)						# User가 Rating한 영화들의 Rating을 numpy array로 저장
    array3 = np.expand_dims(array3, axis=1)							# Rating array을 2차원으로 변환
    array3 = np.tile(array3, (1, num_unrated))							# array3: Rating 배열을 User가 평가하지 않은 영화 개수만큼 반복하여 2차원 array 생성

    k = 3											# 추천할 영화 개수
    array = np.multiply(np.dot(array1, array2.T), array3)					# rate한 영화들과 (array1) rate하지 않은 영화들의 (array2) embedding들을 dot product한 후 array3을 활용한 rate만큼 가중치를 곱함
    array = np.sum(array, axis=0)								# weighted sum하여 최종 array 생성
    index_order = np.argsort(array)[-k:][::-1].tolist()						# index_order: Cosine Similarity을 기준으로 내림차순 정렬된 index_order 생성
    index_order = [x + 1 for x in index_order]							# 인덱스 1부터 시작하도록 조정

    query = f"""
        WITH rated_movies AS (
            SELECT r.mid
            FROM ratings r
            JOIN users u ON r.uid = u.uid
            WHERE u.uid = {uid}
            ORDER BY r.mid
        ),
        unrated_movies AS (
            SELECT m.mid, m.title, m.director, m.nationality, m.release_year, m.profit, m.summary, ROW_NUMBER() OVER (ORDER BY m.mid) AS row_num
            FROM movies m
            WHERE m.mid NOT IN (SELECT mid FROM rated_movies)
            ORDER BY m.mid
        ),
        selected_movies AS (
            SELECT *
            FROM unrated_movies
            WHERE row_num IN {tuple(index_order)}
        )
        SELECT mid, title, director, nationality, release_year, profit, summary
        FROM selected_movies
        ORDER BY CASE row_num
            {''.join([f'WHEN {i} THEN {index_order.index(i)} ' for i in index_order])}
            ELSE row_num
        END; """

    cur.execute(query)										# SQL 쿼리 실행: 앞서 생성한 index_order를 통해 contents-based 추천영화 K개를 추출
    result = cur.fetchall()									# 조회된 모든 행 가져오기
    disconnect(cur, conn)									# PostgreSQL에 연결 종료
    return result										# 조회된 정보 리턴 

# ------------------------ Scenario #3: Collaborative Filtering Recommendation

def recommend_collaborative_filtering(uid):													# Keyboard Input을 통해 받은 collaborative filtering 추천 요청을 수행하기 위한 함수
    conn = connect_to_db()																# PostgreSQL에 연결
    cur = conn.cursor()																	# SQL 쿼리 실행 및 결과 처리를 위한 커서 생성

    target_index = int(uid) - 1																# 추천 대상 User의 인덱스 계산

    cur.execute("SELECT COUNT(DISTINCT uid) FROM users")												# SQL 쿼리 실행: User 수 계산
    num_users = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT mid) FROM movies")												# SQL 쿼리 실행: Movie 수 계산
    num_movies = cur.fetchone()[0]					
    rating_matrix = np.zeros((num_users, num_movies))													# Rating matrix 생성
    cur.execute("SELECT uid, mid, rating FROM ratings")													# SQL 쿼리 실행: 모든 Rating 데이터 가져오기
    ratings = cur.fetchall()
    for uid, mid, rating in ratings: rating_matrix[uid-1][mid-1] = rating										# Rating matrix에 Rating 데이터 매칭시키기

    similarity_matrix = cosine_similarity(rating_matrix)												# Rating matrix의 cosine_similarity를 도출하여 similarity_matrix 생성
    user_similarities = similarity_matrix[target_index]													# 해당 User와 다른 User들의 similarity를 similarity_matrix를 이용하여 구하기
    most_similar_user_index = np.argsort(user_similarities)[-2]												# 해당 User와 가장 유사한 User의 인덱스 추출
    target_user_ratings = rating_matrix[target_index]													# 해당 User의 Rating 정보
    similar_user_ratings = rating_matrix[most_similar_user_index]											# 가장 유사한 User의 Rating 정보

    high_rated_movies = np.where(similar_user_ratings >= 4.5)[0]											# 가장 유사한 User가 4.5 이상으로 Rating한 Movie의 인덱스
    unwatched_movies = np.where(target_user_ratings == 0)[0]												# 해당 User가 아직 보지 않은 영화 인덱스
    recommended_movies = np.intersect1d(high_rated_movies, unwatched_movies)										# 교집합 추출

    if len(recommended_movies) > 0:															# 추천 Movie가 있을 경우
        recommended_movies_list = recommended_movies.tolist()  													# numpy array를 리스트로 변환
        recommended_movies_list = [int(mid) + 1 for mid in recommended_movies_list]  										# 각 요소를 int로 변환하여 mid 생성
        format_strings = ','.join(['%s'] * len(recommended_movies_list))
        query = f"SELECT mid, title, director, nationality, release_year, profit, summary FROM movies WHERE mid IN ({format_strings})"
        cur.execute(query, tuple(recommended_movies_list))													# SQL 쿼리 실행: 추천 영화 정보 조회
        movies = cur.fetchall()
    else:
        movies = []																		# 추천 영화가 없을 경우 빈 리스트 반환

    disconnect(cur, conn)																# PostgreSQL에 연결 종료
    return movies																	# 조회된 정보 리턴 


