import operations as op

if __name__ == "__main__":

    reset = ""
    while (reset != 'y' and reset != 'n'): reset = input("\nReset database? [y/n]: ")
    if (reset == "y"):
        op.drop_existing_tables()
        op.create_tables()
        op.add_data_from_txt()

    n_users = op.count("users")
    n_movies = op.count("movies")
    n_ratings = op.count("ratings")
    print(f"\nLoaded database: {n_users} users, {n_movies} movies, {n_ratings} ratings.\n")

    query = ""
    while (query != "Quit") :
        print("==================================================\n")
        print("Possible commands:")
        print("--------------------------------------------------")
        print("★ Fetch [ user / movie / rating ]")
        print("★ Show [ users / movies / ratings ]")
        print("★ Display [ user history / movie ratings ]")
        print("--------------------------------------------------")
        print("★ Insert [ user / movie / rating ]")
        print("★ Update [ user / movie / rating ]")
        print("★ Delete [ user / movie / rating ]")
        print("--------------------------------------------------")
        print("★ Rank [ weekly / monthly ]")
        print("--------------------------------------------------")
        print("★ Get [ cu / cold-start users ]")
        print("★ Rec [ cb / content-based ]")
        print("★ Rec [ cf / collaborative filtering ]")
        print("--------------------------------------------------")
        print("★ Quit")
        print("\n>>> ", end='')
        query = input()
        print(" ")

        if (query == ""):										
            print("Try again!")
            print(" ")

        elif (query == "Insert user"):										# Keyboard Input이 Insert user인 경우
            uname = input("name: ")											# User 이름 입력 요청 
            gender = input("gender [F/M/U]: ")										# User 성별 입력 요청 
            age = input("age: ")											# User 나이 입력 요청 
            try: 													# 잘못된 값 입력 방지를 위한 예외처리
                ret = op.insert_user(uname, gender, age)									# operations.py의 insert_user 함수 호출하여 User 정보 추가
                print(f"\nInserted user: {ret}\n")										# 추가된 User 정보 출력
            except Exception:												
                print("\nWrong values!\n")											# 예외 발생시에 오류 메시지 출력

        elif (query == "Insert movie"):										# Keyboard Input이 Insert movie인 경우
            title = input("title: ")											# movie 제목 입력 요청 
            director = input("director: ")										# movie 감독 입력 요청 
            nationality = input("nationality: ")									# movie 국가 입력 요청 
            release_year = input("release_year: ")									# movie 연도 입력 요청 
            profit = input("profit: ")											# movie 수익 입력 요청 
            summary = input("summary: ")										# movie 요약 입력 요청 
            try: 													# 잘못된 값 입력 방지를 위한 예외처리
                ret = op.insert_movie(title, director, nationality, release_year, profit, summary)				# operations.py의 insert_movie 함수 호출하여 Movie 정보 추가
                print(f"\nInserted movie: {ret}\n")										# 추가된 Movie 정보 출력
            except Exception:
                print("\nWrong values!\n")											# 예외 발생시에 오류 메시지 출력

        elif (query == "Insert rating"):									# Keyboard Input이 Insert rating인 경우
            uid = input("user id: ")											# rating할 User ID 입력 요청
            try: int(uid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 uid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            mid = input("movie id: ")											# rating 매겨질 Movie ID 입력 요청
            try: int(mid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 mid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            rating = input("rating: ")											# Movie의 rating 입력 요청
            time = input("time: ")											# rating 날짜 입력 요청
            try: 													# 잘못된 값 입력 방지를 위한 예외처리
                ret = op.insert_rating(uid, mid, rating, time)									# operations.py의 insert_rating 함수 호출하여 Rating 정보 추가
                print(f"\nInserted rating: {ret}\n")										# 추가된 Rating 정보 출력
            except Exception:
                print("\nWrong values!\n")											# 예외 발생시에 오류 메시지 출력

        elif (query == "Update user"):										# Keyboard Input이 Update user인 경우
            uid = input("user id: ")											# 갱신할 User ID 입력 요청
            try: int(uid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 uid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            uname = input("name: ")											# 갱신할 User 이름 입력 요청
            gender = input("gender [F/M/U]: ")										# 갱신할 User 성별 입력 요청
            age = input("age: ")											# 갱신할 User 나이 입력 요청
            try: 													# 잘못된 값 입력 방지를 위한 예외처리
                ret = op.update_user(uid=uid, uname=uname, gender=gender, age=age)						# operations.py의 update_user 함수를 호출하여 User 정보 갱신
                print(f"\nUpdated user: {ret}\n")										# 갱신된 User 정보 출력
            except Exception:
                print("\nWrong values!\n")											# 예외 발생시에 오류 메시지 출력

        elif (query == "Update movie"):										# Keyboard Input이 Update movie인 경우
            mid = input("movie id: ")											# 갱신할 Movie ID 입력 요청
            try: int(mid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 mid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            title = input("title: ")											# 갱신할 Movie의 제목 입력 요청
            director = input("director: ")										# 갱신할 Movie의 감독 입력 요청
            nationality = input("nationality: ")									# 갱신할 Movie의 국가 입력 요청
            release_year = input("release_year: ")									# 갱신할 Movie의 연도 입력 요청
            profit = input("profit: ")											# 갱신할 Movie의 수익 입력 요청
            summary = input("summary: ")										# 갱신할 Movie의 요약 입력 요청
            try: 													# 잘못된 값 입력 방지를 위한 예외처리
                ret = op.update_movie(mid=mid, title=title, director=director, 							
                                    nationality=nationality, release_year=release_year, 
                                    profit=profit, summary=summary)								# operations.py의 update_movie 함수를 호출하여 Movie 정보 갱신
                print(f"\nUpdated movie: {ret}\n")										# 갱신된 Movie 정보 출력
            except Exception:								
                print("\nWrong values!\n")											# 예외 발생시에 오류 메시지 출력

        elif (query == "Update rating"):									# Keyboard Input이 Update rating인 경우
            uid = input("user id: ")											# rating을 갱신할 User ID 입력 요청
            try: int(uid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 uid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            mid = input("movie id: ")											# rating이 갱신 될 Movie ID 입력 요청
            try: int(mid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 mid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생	
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력	
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)	
            rating = input("rating: ")
            time = input("time: ")											# rating이 갱신 된 날짜 입력 요청
            try: 													# 잘못된 값 입력 방지를 위한 예외처리
                ret = op.update_rating(uid, mid, rating, time)									# operations.py의 update_rating 함수를 호출하여 Rating 정보 갱신
                print(f"\nUpdated rating: {ret}\n")										# 갱신된 Rating 정보 출력
            except Exception:
                print("\nWrong values!\n")											# 예외 발생시에 오류 메시지 출력

        elif (query == "Delete user"):										# Keyboard Input이 Delete user인 경우
            uid = input("user id: ")											# 삭제할 User의 ID 입력 요청
            try: int(uid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 uid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            op.delete_user(uid=uid)											# operations.py의 delete_user 함수를 호출하여 해당 User 정보 삭제
            print(" ")		

        elif (query == "Delete movie"):										# Keyboard Input이 Delete movie인 경우
            mid = input("movie id: ")											# 삭제할 Movie의 ID 입력 요청
            try: int(mid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 mid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            op.delete_movie(mid=mid)											# operations.py의 delete_movie 함수를 호출하여 해당 movie 정보 삭제
            print(" ")

        elif (query == "Delete rating"):									# Keyboard Input이 Delete rating인 경우
            uid = input("user id: ")											# 삭제할 rating을 입력한 User의 ID 입력 요청
            try: int(uid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 uid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            mid = input("movie id: ")											# 삭제할 rating의 Movie ID 입력 요청
            try: int(mid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 mid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            op.delete_rating(uid=uid, mid=mid)										# operations.py의 delete_movie 함수를 호출하여 해당 rating 정보 삭제
            print(" ")

        elif (query == "Fetch user"):										# Keyboard Input이 Fetch user인 경우
            uid = input("user id: ")											# 조회할 User ID 입력 요청	
            try: int(uid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 uid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            ret = op.fetch_user(uid)											# operations.py의 fetch_user 함수를 호출하여 User 정보 조회
            print(f"\nFetched user: {ret}\n")										# 조회된 User 정보 출력

        elif (query == "Fetch movie"):										# Keyboard Input이 Fetch movie인 경우
            mid = input("movie id: ")											# 조회할 Movie ID 입력 요청
            try: int(mid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 mid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            ret = op.fetch_movie(mid)											# operations.py의 fetch_movie 함수를 호출하여 Movie 정보 조회
            print(f"\nFetched movie: {ret}\n")										# 조회된 Movie 정보 출력

        elif (query == "Fetch rating"):										# Keyboard Input이 Fetch rating인 경우
            uid = input("user id: ")											# 조회할 User ID 입력 요청
            try: int(uid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 uid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            mid = input("movie id: ")											# 조회할 Movie ID 입력 요청
            try: int(mid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 mid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생	
                print("\nWrong value!\n")											# 사용자에게 오류 메시지 출력 
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료) 
            ret = op.fetch_rating(uid, mid)										# operations.py의 fetch_rating 함수를 호출하여 Rating 정보 조회
            print(f"\nFetched rating: {ret}\n")										# 조회된 Rating 정보 출력

        elif (query.split()[0] == "Show"):									# Keyboard Input이 Show로 시작되는 경우
            if (len(query.split()) > 1):										# Keyboard Input에 테이블 (users, movies, ratings) 이름이 포함된 경우
                try: ret = op.show(query.split()[1])										# operations.py의 show 함수 호출하여 해당 테이블 데이터 조회
                except Exception: print(f"{Exception}!\n")									# 잘못된 테이블 이름 입력과 같은 예외 발생시에 오류 메시지 출력
            if (ret):													# 조회 결과가 있는 경우에는
                for r in ret:													# 조회된 각 행에 대해 for문을 돌려서
                    print(r, end='\n')													# 행 데이터 출력
                num = op.count(query.split()[1])										# 해당 테이블의 행 개수 계산
                print(f" {num} rows in total.\n")										# 계산된 총 행 개수 출력
            else: print("Try again!\n")											# 조회 결과가 없는 경우에는 오류 메시지 출력

        elif (query == "Display user history"):									# Keyboard Input이 Display user history인 경우
            uid = input("user id: ")											# 조회할 User ID 입력 요청
            try: int(uid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 uid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nTry again!\n")												# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            ret = op.display_user_history(uid)										# operations.py의 display_user_history 함수를 호출하여 User 기록 조회
            print(" ")													# 빈 줄 출력
            if (ret and ret[0]):											# Movie 조회 결과가 있고, User 정보가 존재하는 경우
                print(f"User information: {ret[0]}\n")										# User 정보 출력
                print(f"Activity history: ")											# Movie 조회 이력 출력을 구분 하기 위한 헤더 출력
                for r in ret[1]:						
                    print(r, end='\n')													# Movie 조회 이력 출력
            else: print("Cannot find user!")										# User 정보가 존재하는지 않는 경우 오류 메시지 출력
            print(" ")													# 빈 줄 출력

        elif (query == "Display movie ratings"):								# Keyboard Input이 Display user history인 경우
            mid = input("movie id: ")   										# 조회할 Movie ID 입력 요청
            try: int(mid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 mid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생 
                print("\nTry again!\n")												# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            print(" ")													# 빈 줄 출력
            ret = op.display_movie_ratings(mid)										# operations.py의 display_movie_ratings 함수를 호출하여 Movie 기록 조회
            if (ret and ret[0]):											# User가 Rating한 이력이 있고, Movie 정보가 존재하는 경우
                print(f"Movie information: {ret[0]}\n")										# Movie 정보 출력
                print(f"Activity history: ")											# User가 Rating한 이력 출력을 구분 하기 위한 헤더 출력
                for r in ret[1]:						
                    print(r, end='\n')													# User가 Rating한 이력 출력
            else: print("Cannot find movie!")										# Movie 정보가 존재하는지 않는 경우 오류 메시지 출력
            print(" ")													# 빈 줄 출력
        
        elif (query == "Rank weekly"):										# Keyboard Input이 Rank weekly인 경우
            results = op.show_weekly_ranking()										# operations.py의 show_weekly_ranking 함수를 호출하여 주간 Movie 순위 조회
            for row in results:				
                print(row)													# 조회된 정보를 각 행별로 출력
                
        elif (query == "Rank monthly"):										# Keyboard Input이 Rank monthly인 경우
            results = op.show_monthly_ranking()										# operations.py의 show_monthly_ranking 함수를 호출하여 월간 Movie 순위 조회
            for row in results:				
                print(row)													# 조회된 정보를 각 행별로 출력

        elif (query in ["Get cold-start users", "Get cu"]):							# Keyboard Input이 Get cold-start users 혹은 Get cu인 경우
            ret = op.get_cold_start()											# operations.py의 get_cold_start 함수를 호출하여 cold-start users 조회
            for r in ret:			
                print(r, end='\n')												# 조회된 cold-start User 정보를 한 줄씩 출력
            print(" ")

        elif (query in ["Rec content-based", "Rec cb"]):							# Keyboard Input이 Rec content-based 혹은 Rec cb인 경우
            uid = input("user id: ")											# 추천 받을 User ID 입력 요청
            try: int(uid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 uid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nTry again!\n")												# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            print(" ")
            ret = op.display_user_history(uid)										# operations.py의 display_user_history 함수를 호출하여 User 기록 조회

            if (ret and ret[0]):											# User 정보와 Rating 기록이 존재하는 경우
                print(f"User information: {ret[0]}\n")										# User 정보 출력
                print(f"Activity history: ")											# Rating 기록 출력
                for r in ret[1]:
                    print(r, end='\n')
                print("\nRecommended Movies:")											# 추천 영화 목록 출력 헤더 
                ret = op.recommend_contents_based(uid)										# operations.py의 recommend_contents_based 함수를 호출하여 contents based 추천 영화 목록 가져오기
                for r in ret: print(r)												# contents based 추천 영화 목록 출력
            else: print("Cannot find user!")										# User 정보 또는 Rating 기록이 존재하지 않는 경우에는 오류 메시지 출력
            print(" ")

        elif (query in ["Rec collaborative filtering", "Rec cf"]):						# Keyboard Input이 Rec collaborative filtering 혹은 Rec cf인 경우
            uid = input("user id: ")											# 추천 받을 User ID 입력 요청
            try: int(uid)												# 사용자가 정수가 아닌 값을 입력할 경우에는 입력된 uid를 정수로 변환 시도
            except ValueError:												# 정수 변환 실패 시 예외 발생
                print("\nTry again!\n")												# 사용자에게 오류 메시지 출력
                continue													# 다음 명령어 입력 대기 (현재 명령어 처리 종료)
            print(" ")
            ret = op.display_user_history(uid)										# operations.py의 display_user_history 함수를 호출하여 User 기록 조회

            if (ret and ret[0]):											# User 정보와 Rating 기록이 존재하는 경우
                print(f"User information: {ret[0]}\n")										# User 정보 출력
                print(f"Preferred Movies: ")											# User가 선호하는 영화 출력 헤더  
                for r in ret[1]:												# Rating 4.0 이상의 User가 선호하는 영화 출력				
                    if (r[2] >= 4.0): print(r, end='\n')	
                print("\nRecommended Movies:")											# 추천 영화 목록 출력 헤더 
                ret = op.recommend_collaborative_filtering(uid)									# operations.py의 recommend_collaborative_filtering 함수를 호출하여 collaborative filtering 기반 추천 영화 목록 가져오기
                for r in ret: print(r)												# collaborative filtering 기반 추천 영화 목록 출력
            else: print("Cannot find user!")										# User 정보 또는 Rating 기록이 존재하지 않는 경우에는 오류 메시지 출력
            print(" ")

        elif (query.split()[0] != "Quit"):
            print("Try again!")
            print(" ")

    print("========================================\n")
    print("Good bye.\n")
