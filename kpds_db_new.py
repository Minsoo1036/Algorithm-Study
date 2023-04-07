# Ver.2.0 (211012)
# Ver.2.1 (211020)
# Ver.2.2 (211101, 코멘트 수정)
# Ver.2.22 (211103, df_decomp에 datetime column type 변경 옵션 추가)
# Ver.2.23 (211118, all_date freq=="DS"인 경우, 주말 제외)
# Ver.2.24 (211123, stat_search, price_search)
# Ver.2.25 (211220, data_freq 빈도 기준으로 변경, all_date과 complete_df 재정의, m_to_week complete_df에 흡수)
# Ver.2.26 (220103, hist_err 양식 변경, 관련 에러 발생시 2.26버전으로 통일)
# Ver.2.27 (220106, df_maker col 이름 set/list/tuple인 경우 str의 join값으로 통일, sort by Date 추가)
# Ver.2.28 (220111, data_freq에 exception 옵션 추가, all_date에 응용)
# Ver.2.29 (220211, 모든 가격, 모든 통계, 모든 지수 업로드 함수 개발중)
# Ver.2.30 (220301, pr_q 수정)
# 작정일자: 22/02/11 (cx_Oracle 추가 버젼, connection on-and-off)
# 작정인: 김건
# 최종 수정일자: 21/12/06

import numpy as np
import pandas as pd
import cx_Oracle, pyodbc
import pymongo
import datetime, sys, glob

from dateutil.relativedelta import relativedelta
from tsmoothie.smoother import LowessSmoother

#----------------------------------------------------
# Oracle DB 데이터 로드 함수

def std_dbcode(dbcode):
    """
    코드 앞에 1_, 2_, 4_ 가 앞에 붙이있는 경우 제거하고 코드 사이 _를 -로 통일
    """
    if dbcode.strip().startswith(("1_", "2_", "4_")):
        dbcode = dbcode.strip()[2:]
    return(dbcode.replace('_', '-').strip())

def sql_gen(code, db_tbl, cd_cols, cols = ['*']):
    """
    SQL query문을 작성해주는 함수
    """
    query = "SELECT " + ", ".join(cols) + " FROM " + db_tbl + " WHERE "
    return(query + " AND ".join(["{} = '{}'".format(a_, b_) for a_, b_ in zip(cd_cols, std_dbcode(code).split('-'))]))

# def query_basic(q_line, order = "REG_DATE", pac = "pyodbc"): 
#     """
#     query문(string) to dataframe. 더 빠른 pyodbc가 default
#     """
#     if pac == "pyodbc":
#         dsn, username, password = 'comm', 'comm', 'koreapds' 
#         con = pyodbc.connect('DSN=' + dsn  + ';UID=' + username + ';PWD=' + password)
#     else:
#         con = cx_Oracle.connect("comm/koreapds@222.231.1.83:1521/koreapds")
#     if type(order) == str:
#         q_line = q_line +" ORDER BY "+ order
#     df = pd.read_sql(q_line, con)
#     con.close()
#     return(df)

def query_basic(q_line, order = "REG_DATE", pac = "pyodbc"): 
    """
    query문(string) to dataframe. 더 빠른 pyodbc가 default
    """
    if pac == "pyodbc":
        dsn, username, password = 'comm', 'comm_op', 'kpds#3337' 
        con = pyodbc.connect('DSN=' + dsn  + ';UID=' + username + ';PWD=' + password)
    else:
        con = cx_Oracle.connect("comm_op/kpds#3337@222.231.1.83:1521/koreapds")
    if type(order) == str:
        q_line = q_line +" ORDER BY "+ order
    df = pd.read_sql(q_line, con)
    con.close()
    return(df)

def pr_q(dbcode, cols = ['*']):
    """
    KPDS Oracle DB 가격/통계 자료 로드 기본 함수. 로드 할 columns 지정 가능
    """
    dbcode = std_dbcode(dbcode)
    if len(dbcode.split("-")) == 8 :
        q_line = sql_gen(dbcode, "comm.TBL_COMM_PRICE_INFO", 
                         ["LARGE_CD","MIDDLE_CD","SMALL_CD","DISTR_CD",
                          "ITEM_CD","ITEMD_CD","DEAL_CD",'GAP_CD'], cols)
        if q_line[-4:] == "'NO'":
            q_line = q_line.replace("GAP_CD = 'NO'", "CORE_CD = '01'")
    elif len(dbcode.split("-")) == 10:
        q_line = sql_gen(dbcode, "comm.TBL_COMM_STATS_INFO",
                         ["STATS_LARGE_CD","STATS_MIDDLE_CD","STATS_SMALL_CD",
                          "STATS_ITEM_CD","STATS_ITEMD_CD","ZONE_CD",
                          "NATION_CD","COMPANY_CD","ORGAN_CD","PERIOD_CD"], cols) + " AND REVISE_CD = '9'"
    else:
        return(pd.DataFrame(columns = ["Date", "Data"]))
    return(query_basic(q_line))

def pr_q_fx(target, base = "USD", round_n = 3):
    """
    KPDS Oracle DB 환율 자료 로드 기본 함수
    """
    df = query_basic(sql_gen(std_dbcode(base+"-"+target), "comm.TBL_COMM_FXRATE_INFO", 
                             ["FX_BASE_CD", "FX_TARGET_CD"], cols = ["FX_REG_DATE", "FX_FXRATE"]), order="FX_REG_DATE")
    if len(df.index)>0:
        df['Data'] = round(df['FX_FXRATE'], round_n)
        df['Date'] = df['FX_REG_DATE'].dt.date
        return(df[["Date","Data"]])
    else:
        return(pd.DataFrame(columns = ["Date", "Data"]))
    
def pr_q_slim(dbcode, cols = []):
    """
    KPDS Oracle DB 자료 로드 통합 함수. Slim 데이터 ( [['Date', "Data"]] 형식 )
    """
    dbcode, s_col = std_dbcode(dbcode), "JISU"
    if (len(dbcode)<14  and dbcode.isdigit()): # 지수 여부 체크, 지수 로드
        df = query_basic( sql_gen(dbcode, "comm.TBL_COMM_INDEX_JISU_HISTORY", ["JISU_CD"], cols = ["REG_DATE", s_col]) )
    elif len(dbcode) in [7, 3]:  # 환율 여부 체크, 환율 로드
        if len(dbcode) == 7:
            fx_code, base = dbcode.split("-")[1], dbcode.split("-")[0]
        else:
            fx_code, base = dbcode, "USD"
        temp_df = pr_q_fx(fx_code, base = base)
        if len(temp_df.index) == 0:
            temp_df = pr_q_fx(base, base = fx_code)
        return(temp_df)
    elif len(dbcode.split("-")) in [8, 10]:
        s_col = {8:"PRICE", 10:"STATS_VALUE"}[len(dbcode.split("-"))]
        cols = [x for x in cols if x not in ["REG_DATE", s_col]]
        df = pr_q(dbcode, cols = ["REG_DATE", s_col] + cols)
    else:
        return(pd.DataFrame(columns = ["Date", "Data"]))
    if len(df.index) == 0:
        return(pd.DataFrame(columns = ["Date", "Data"]))
    df["Date"] = df["REG_DATE"].dt.date
    return(df[[x for x in ["Date", s_col] + cols if x in list(df) ] ].rename(columns = {s_col:"Data"})   )

# def news_query(code, news = "Daily", seq_i = -1):
#     """
#     KPDS Oracle DB 내 뉴스 기사 로드 함수
#     """
#     if "daily" in news.lower():
#         if seq_i == -1 or type(seq_i) != int: seq_i = 25000
#         query = "SELECT A.SEQ, B.REG_DATE, B.TITLE, A.CONTENT FROM comm.TBL_COMM_REPORT_MARKET_SUB A, comm.TBL_COMM_REPORT_MARKET B WHERE A.SEQ = B.SEQ AND A.SEQ>"+str(seq_i)+" AND "
#         p_cols = ["A.LARGE_CD","A.MIDDLE_CD","A.SMALL_CD","A.DISTR_CD",
#                   "A.ITEM_CD","A.ITEMD_CD","A.DEAL_CD"]
#     elif "spot" in news.lower():
#         if seq_i == -1 or type(seq_i) != int: seq_i = 100000
#         query = "SELECT SEQ, TITLE, CONTENT, REG_DATE FROM comm.TBL_COMM_NEWS_CHOICE WHERE SEQ>"+str(seq_i)+" AND REG_USER != 'AUTO' AND "
#         p_cols = ["LARGE_CD","MIDDLE_CD","SMALL_CD","DISTR_CD","ITEM_CD"]
#     elif "econ" in news.lower():
#         if seq_i == -1 or type(seq_i) != int: seq_i = 3500
#         return(query_basic("SELECT TITLE, CONTENT, REG_DATE FROM comm.TBL_COMM_CALENDAR_LOOK WHERE CAL_SEQ>"+str(seq_i)))
#     else:
#         print("뉴스는 Daily, SPOT, Econ 중 하나여야 합니다")
#         return(pd.DataFrame(columns = ["CONTENT", "REG_DATE"]))
#     for cd_col in p_cols:
#         query = query + cd_col + " = '" + code.split('-')[p_cols.index(cd_col)] + "' AND "
#     return(query_basic(query.rsplit(" AND ", 1)[0]))


#----------------------------------------------------
# MongoDB 업로드 주요 가격 리스트 관련 함수

def get_items(list_name = "섹터별 주요 품목"):
    """
    리서치DB에 저장된 변수 리스트 로드 함수. Output은 pandas DataFrame
    """
    userid, password = 'comm', 'koreapds'
    client = pymongo.MongoClient(f'mongodb://{userid}:{password}@192.168.0.124:27017/commodity')

    if client['commodity']['dbcode_list'].count_documents({"name":list_name}) != 1:
        print("품목 목록 "+list_name + "은 존재하지 않습니다. 이하 목록명에서 선택하세요")
        print([x['name'] for x in client['commodity']['dbcode_list'].find()])
        return(pd.DataFrame([]))
    else:
        # MongoDB에서 섹터별 주요 품목 가져오기
        document = client['commodity']['dbcode_list'].find_one({"name":list_name})
    return(pd.DataFrame(document['df'], columns = document['cols']))

def get_item_list(list_name = "섹터별 주요 품목"):
    """
    get_items로 로드한 변수 리스트(pandas DataFrame)를 dbcode dictionary 화
    """
    i_df, item_list = get_items(list_name), {}
    for idx in range(len(i_df.index)):
        item_list[(i_df.iloc[idx]['item_name'], i_df.iloc[idx]['item_spec'])] = i_df.iloc[idx]['dbcode']
    return(item_list)

def get_df(item_name = "전기동", item_spec = "-1", list_name = "전 전망 품목", dbcode = False):
    """
    개별 품목 가격 데이터 로드 간소화. dbcode = True 옵션으로 dbcode 로드 가능
    """
    df = get_items(list_name = list_name)
    if item_spec == "-1":
        df = df.sort_values('item_spec').drop_duplicates('item_name')
    else:
        df = df[(df["item_name"]==item_name)&(df["item_spec"]==item_spec)]
    if len(df[(df["item_name"]==item_name)].index) ==0:
        return(pd.DataFrame(columns = ['Date', "Data"]))
    if dbcode:
        return(df[(df["item_name"]==item_name)].iloc[0]['dbcode'])
    else:
        return(pr_q_slim(df[(df["item_name"]==item_name)].iloc[0]['dbcode']))


#----------------------------------------------------
# 데이터 빈도 자동 계산 + 적절한 평균 도출 함수

def int_to_freq(num_val, count = True, exc = False):
    """
    연간 데이터 개수(int) 입력 시 데이터 업데이트 빈도(str)를 return
    """
    if count:
        order_dict = {0:"YS", 2:"QS", 4:"MS", 6:"WS", 8:"DS"}
        board = [1.5, 3.5, 4.5, 10.5, 13.5, 44.5, 55.5, 150.5]
    else:
        order_dict = {0:"DS", 2:"WS", 4:"MS", 6:"QS", 8:"YS"}
        board = [4.5, 4.6, 8.5, 27.5, 32.5, 88.5, 95.5, 360.5, 369.5]
    temp_li = sorted(board + [num_val])
    if temp_li.index(num_val) in list(order_dict):
        return(order_dict[temp_li.index(num_val)])
    else:
        if exc:
            return("NA")
        else:
            print("에러 발생: freqency를 확인할 수 없습니다. 스크립트를 종료합니다")
            sys.exit()

def year_prof(df_o, cols = "Data"):
    """
    pr_q_slim으로 로드한 가격/통계 데이터 연간 프로필. Output은 pandas DataFrame
    """
    df, col = reset_date(df_o), df_cols(df_o, cols = cols)[-1]
    df['year'] = df['Date'].map(lambda x: x.year)
    df = df[['year', col]].groupby(['year']).count().reset_index()
    df['freq'] = df[col].map(lambda x: int_to_freq(x, exc = True))
    return(df.sort_values(by = "year"))

def data_freq(df_o, recent = True, exc = True):
    """
    input: 가격/통계 DataFrame, output: freq, 데이터 빈도 str
    """
    df = reset_date(df_o)
    if recent:
        df = df[df["Date"]>=datetime.date(df['Date'].max().year - 2, 1, 1)]
    df['diff'] = df['Date'].diff(1).fillna(method = "bfill").map(lambda x: int_to_freq(x.days, count = False, exc = True))
    freq  = df.groupby("diff").count().sort_values(by = "Date").iloc[-1:].index[0]
    if freq == "NA" and exc== True:
        print("에러 발생: 데이터의 freqency를 확인할 수 없습니다. 스크립트를 종료합니다")
        display(df_o.head())
        display(df_o.tail())
        sys.exit()
    return(freq)

def lower_freq(freq, reverse = False):
    """
    데이터 빈도(str) 입력 시 더 높은 빈도 str으로 구성된 list를 return, reverse 가능
    """
    if reverse:
        return(['YS', 'QS', 'MS', 'WS', 'DS'][:['YS', 'QS', 'MS', 'WS', 'DS'].index(freq) + 1])
    else:
        return(['YS', 'QS', 'MS', 'WS', 'DS'][['YS', 'QS', 'MS', 'WS', 'DS'].index(freq):])


#----------------------------------------------------
# 데이터 평균 계산 함수

def to_datetime(date, date_form = "%Y-%m-%d"):
    """
    datetime.date()이나 str을 datetime.datetime()화. MongoDB 업로드에 필수로, df_decomp에 활용
    """
    if type(date) == datetime.date:
        return(datetime.datetime.combine(date, datetime.datetime.min.time()))
    elif type(date) == str and len(date.split("-")) == 3:
        return(datetime.datetime.strptime(date, date_form))
    else:
        return(date)

def df_cols(df_o, cols = True):
    """
    input은 pandas DataFrame(dictionary도 가능함). cols 안의 element 중 input df에 포함된 column을 return
    """
    if type(cols) in [set, list, tuple]:
        return([x for x in cols if x in list(df_o) and x != "Date"])
    elif cols in list(df_o):
        return([ cols ])
    else:
        return([ x for x in list(df_o) if x!= 'Date'])

def to_date(date_in, date_form = "%Y-%m-%d"):
    """
    datetime.datetime()이나 str을 datetime.date()화
    """
    if type(date_in) == str and len(date_in.split("-")) == 3:
        return(datetime.datetime.strptime(date_in, date_form).date())
    else:
        try:
            return(date_in.date())
        except:
            return(date_in)

def datecol_dt(df_o, col = "Date"):
    """
    Date column을 datetime.date()화
    """
    df = df_o.copy()
    try:
        df["Date"] = df[col].dt.date
    except:
        pass
    return(df)
    
def set_date(df_o):
    """
    Date column이 있을 경우 이를 index화
    """
    df = datecol_dt(df_o)
    if df.index.name != "Date" and "Date" in list(df):
        df = df.set_index("Date").sort_index()
    return(df)

def reset_date(df_o):
    """
    날짜가 index인 경우 Date column으로 빼냄
    """
    if "Date" not in list(df_o) and type(to_date(df_o.index[0])) == datetime.date:
        return(datecol_dt(df_o.reset_index().rename(columns = 
                                                    {df_o.index.name:"Date", "index":"Date"})))
    else:
        return(df_o.copy())

def stre_date(df_o, freq = "DS", start = -1, end = -1):
    """
    input: 가격/통계 DataFrame과 freq, output: freq 기준 모든 날짜 Date로 구성된 DataFrame
    """
    df = reset_date(df_o)
    if len(df.index) == 0:
        return(pd.DataFrame([]))
    min_d, max_d = to_date(df['Date'].min()), to_date(df['Date'].max())
    if type(to_date(end)) == type(to_date(df['Date'].min())):
        max_d = to_date(end)
    if type(to_date(start)) == type(to_date(df['Date'].min())):
        min_d = to_date(start)
    date_df = pd.DataFrame(pd.date_range(min_d, periods=(max_d - min_d).days + 1), columns = ['Date'])
    date_df['Date'] = date_df['Date'].dt.date
    return(dat_ave(date_df.reset_index(), freq = freq).drop("index", axis = 1))

def week_ave(df_o, standard = 0, mon_rep = True, cols = True, maxmin = False):
    """
    자유도 높은 월간 평균 도출 함수. 기본은 월요일 날짜로 통일
    """
    df = reset_date(df_o)
    # 과거 현재 날짜 만들기
    df = pd.merge(stre_date(df, freq = "DS", start = df['Date'].min() - relativedelta(days = df['Date'].min().weekday())),
                  df, on = "Date", how = 'left')

    # Column 별 주간 평균 구하기
    if not mon_rep or type(mon_rep) != bool:
        mon_rep = -1
    for col in df_cols(df, cols = cols):
        df_p = df[["Date", col]].copy()
        for ind in range(7):
            df_p[col+'_'+str(ind)] = df_p[col].shift(-ind * mon_rep)
        df_p = df_p.dropna(subset=[col+'_'+str(x) for x in range(7)], how = 'all')
        df_p[col] = round(df_p[[col+'_'+str(x) for x in range(7)]].mean(axis = 1), 3)
        # 필요한 경우 Max Min 추가
        if maxmin:
            df_p[col+"_max"] = df_p[[col+'_'+str(x) for x in range(7)]].max(axis = 1)
            df_p[col+"_min"] = df_p[[col+'_'+str(x) for x in range(7)]].min(axis = 1)
        if "r_df" in locals():
            r_df = pd.merge(r_df, df_p[[x for x in ["Date", col, col+"_max", col+"_min"] if x in list(df_p)]], 
                            on = "Date", how = 'outer')
        else:
            r_df = df_p[[x for x in ["Date", col, col+"_max", col+"_min"] if x in list(df_p)]].copy()
    r_df = r_df[r_df["Date"]>=r_df.dropna()['Date'].min()]
    if type(standard) == int:
        return(r_df[r_df["Date"].map(lambda x: x.weekday())==standard].sort_values("Date").reset_index(drop = True))
    else:
        return(r_df[r_df["Date"].isin(list(standard['Date']))].sort_values("Date").reset_index(drop = True))

def dat_ave(df_o, freq = "MS", how = "mean"):
    """
    WS, MS, QS, YS 평균 도출 함수. freq == DS 시 input df return. 외부 데이터 로드 시 데이터 type이 float 아니면 오류 발생. 
    """
    if freq == "WS":
        return(week_ave(df_o))
    elif freq == "DS":
        return(reset_date(df_o))
    df = reset_date(df_o)
    df.index = pd.to_datetime(df["Date"])
    if how =="mean":
        df = round(df[df.columns.difference(['Date'])].resample(freq).mean(), 3)
    elif how == "sum":
        df = round(df[df.columns.difference(['Date'])].resample(freq).sum(), 3)
    df = df.reset_index()
    df['Date'] = df['Date'].map(lambda x: datetime.date(x.year, x.month, 1))
    return(df.sort_values(by = "Date").reset_index(drop = True))

def all_date(df_o, freq = "WS", method = "linear", order = 1, adj = True):
    """
    input: freq가 통일된 가격/통계 DataFrame, output: freq가 통일된, 모든 날짜가 채워진 가격/통계 DataFrame
    """
    if len(df_o.index) == 0:
        return(pd.DataFrame([]))
    df, freq_o = reset_date(df_o), data_freq(df_o, exc = False)
    if freq_o in ["MS", "QS", "YS"]:
        df["Date"] = df["Date"].map(lambda x: x + relativedelta(days = 14 - x.weekday()))
    df = pd.merge(stre_date(df, freq = freq), dat_ave(df, freq = freq), on = "Date", how = "left")
    if freq == "DS":
        df = df[df['Date'].map(lambda x: x.weekday())<5]
    if method in ["spline", "linear", "slinear", "quadratic"]:
        df = round(set_date(df).interpolate(method = method, order = order), 3)
    else:
        if method not in ["ffill", "bfill"]:
            method = "ffill"
        df = round(set_date(df).fillna(method = method), 3)
    if adj and freq_o =="MS" and freq in ["WS", "DS"]:
        sc_board = add_ym(pd.merge(reset_date(df_o), 
                                   dat_ave(df).rename(columns = {"Data":'adj'}), on = 'Date'))
        sc_board['adj'] = sc_board['Data'] - sc_board['adj']
        df = pd.merge(add_ym(reset_date(df)), sc_board[['adj', 'Year', 'Month']], 
                      on = ['Year', 'Month'], how = 'outer')
        df['adj'] = np.where(df['adj'].isna(), 0, df['adj'])
        df['Data'] = df['Data'] + df['adj']
        df = df.drop(["adj", 'Year', 'Month'], axis = 1)
    return(reset_date(df).sort_values(by = "Date"))

def complete_df(df_o, freq = "WS", method = "linear", order = 1, adj = True):
    """
    major freq 변경 있는 가격/통계 DataFrame까지 빈 날짜 채우고 freq 통일(all_date 사용, m_to_week 대체)
    input: 가격/통계 DataFrame, output: freq가 통일된, 모든 날짜가 채워진 가격/통계 DataFrame
    """
    if len(df_o.index) <= 1:
        return(df_o.copy())
    df = reset_date(df_o)
    df['diff'] = df['Date'].diff(-1).fillna(method = "ffill")
    df['diff'] = df['diff'].map(lambda x: int_to_freq(abs(x.days), count = False, exc = True))
    try:
        f_da = max(df[df['diff'].isin(lower_freq("WS"))].iloc[0]["Date"],
                   df[~df['diff'].isin(lower_freq("WS"))].iloc[0]["Date"])
    except:
        f_da = df['Date'].max() + relativedelta(days = 1)
    if f_da <= df['Date'].iloc[1]:
        f_da = df['Date'].max() + relativedelta(days = 1)
    df = df.drop("diff", axis = 1)
    df = pd.concat([all_date(df[df['Date']<f_da], freq = freq, method = method, order = order, adj = adj),
                    all_date(df[df['Date']>=f_da], freq = freq, method = method, order = order, adj = adj)], sort= False)
    df = set_date(df).sort_index()
    return(reset_date(df[~df.index.duplicated(keep='last')].sort_index()))


#----------------------------------------------------
# 데이터 처리 함수

def add_ym(df_o, col = "Date"):
    """
    Year와 Month 추가
    """
    df = reset_date(df_o)
    df["Year"] = df["Date"].map(lambda x: x.year)
    df["Month"] = df["Date"].map(lambda x: x.month)
    return(df)

def add_yoy(df_o, cols = -1 , yoy = True):
    """
    YoY 포함된 DataFrame 만들기. output은 pandas DataFrame
    """
    fin_df = pd.DataFrame(columns = ["Date"])
    for col in df_cols(df_o, cols = cols):
        df, df_pr = add_ym(df_o), add_ym(df_o).rename(columns = {col:"O_"+col})
        df["Year"] = df["Year"] - 1
        df = pd.merge(df, df_pr[["Year", "Month", "O_"+col]], on = ["Year", "Month"], how = 'left')
        if yoy:
            df[col+"_YoY"] = round((df[col]-df["O_"+col])/df["O_"+col]*100, 3)
        else:
            df[col+"_YoY"] = round((df[col]-df["O_"+col]), 3)
        fin_df = pd.merge(fin_df, df[["Date", col, col+"_YoY"]], on = "Date", how = "outer")
    return(fin_df)

def add_data(df_o, val = np.nan, freq = -1):
    """
    DataFrame 다음 날짜 데이터 입력. freq 입력하지 않을 시 자동 식별. output은 pandas DataFrame
    """
    df = set_date(df_o)
    if type(freq) != str:
        freq = data_freq(df_o)
    if "M" in freq:
        df.loc[df_o['Date'].max() + relativedelta(months = 1)] = val
    elif "W" in freq:
        df.loc[df_o['Date'].max() + relativedelta(days = 7)] = val
    elif "Q" in freq:
        df.loc[df_o['Date'].max() + relativedelta(months = 3)] = val
    elif "Y" in freq:
        df.loc[df_o['Date'].max() + relativedelta(years = 1)] = val
    else:
        next_date = df_o['Date'].max() + relativedelta(days = 1)
        if next_date.weekday() > 4:
            next_date = next_date + relativedelta(days = (7 - next_date.weekday()))
        df.loc[next_date] = val
    return(df.reset_index().sort_values(by = "Date"))


#----------------------------------------------------
# 데이터 dictionary와 df화 함수들(LSTM 등 사용에 특히 필요)

def gen_df_dict(item_dict):
    """
    input: get_item_list 등으로 만든 dbcode의 dictionary. output: pandas DataFrame의 dictionary
    """
    df_dict = {}
    for item in list(item_dict):
        df_dict[item] = pr_q_slim(item_dict[item]).copy()
    return(df_dict)

def df_maker(df_dict, cols = -1, col_name = "Data", freq = "WS", how = "mean"):
    """
    input: pandas DataFrame의 dictionary, output: freq 기준으로 평균/합을 계산한 pandas DataFrame
    """
    if type(df_cols(df_dict, cols = cols)[0]) in [tuple, set, list]:
        df_dict_u = {"_".join([str(x) for x in k]):v for k,v in df_dict.items()}
        cols = ["_".join([str(x) for x in k]) for k in df_cols(df_dict, cols = cols)]
    else:
        df_dict_u = df_dict.copy()
        cols = df_cols(df_dict, cols = cols)
    res_ = pd.DataFrame(columns = ['Date'])
    for col in cols:
        res_ = pd.merge(res_, df_dict_u[col].rename(columns = {col_name:col}), on = 'Date', how = "outer")
    return(dat_ave(res_, freq = freq, how = how).sort_values(by = "Date"))

def df_smoother(df_o, cols = -1, smooth_fraction=0.1, iterations=1):
    """
    input: 시계열 pd DataFrame, output: 스무딩 된 데이터 포함 pd DataFrame, cols를 통해 column 지정 가능
    """
    df, res_ = set_date(df_o).dropna(), set_date(df_o).reset_index().dropna()
    smoother = LowessSmoother(smooth_fraction=smooth_fraction, iterations=iterations)
    for col in df_cols(df, cols = cols):
        smoother.smooth(df[col])
        res_ = pd.merge(res_, pd.DataFrame(smoother.smooth_data, index = [col+"_Smth"]).T, 
                        left_index=True, right_index=True)
    return(res_)

def coll_profile(model_col):
    """
    input: MongoDB collection 이름 str, output: collection 내 item_name/item_spec/dbcode 별 dcoument 숫자 pd DataFrame
    """
    stats_df, count = [], 1
    if model_col.count_documents({"dbcode":{"$exists":True}}) == 0:
        return(pd.DataFrame([], columns = ['item_name', 'item_spec', "dbcode", 'count']))
    dbcode_list, stats_df = [], []
    while True:
        sample = model_col.find_one({'dbcode':{"$nin":dbcode_list}})
        if type(sample) != dict:
            break
        dbcode_list = dbcode_list + [sample["dbcode"]]
        stats_df.append({"item_name":sample["item_name"], "item_spec":sample["item_spec"],
                         "dbcode":sample["dbcode"], "count":model_col.count_documents({'dbcode':sample["dbcode"]})})
    return(pd.DataFrame(stats_df))


# --------------------------------------------------
# KPDS 과거 전망치 로드 함수

def fc_load(dbcode, pred_date = datetime.date(2019,1,21), exact = True):
    """
    input: 품목 dbcode str & 전망시점 pred_date, output: 레거시 모델 전망치 pd DataFrame
    """
    dbcode = std_dbcode(dbcode) # dbcode 표준화
    cols = ["LARGE_CD","MIDDLE_CD","SMALL_CD","DISTR_CD",
            "ITEM_CD","ITEMD_CD","DEAL_CD",'GAP_CD']
    if len(dbcode.split("-")) != 8 :
        return(pd.DataFrame([]))
    q_line = sql_gen(dbcode, "STAT.TBL_PAMS_MODEL_PRICE_MONTH12_R", cols, ["*"])
    if q_line[-4:] == "'NO'":
        q_line = q_line.replace("GAP_CD = 'NO'", "CORE_CD = '01'")
    q_line += " AND REG_DATE >= to_date('"+str(to_date(pred_date))+"', 'yyyy-mm-dd')"
    if exact:
        q_line = q_line.replace("REG_DATE >= to_date", "REG_DATE = to_date")
    df = query_basic(q_line)
    return(df[[x for x in list(df) if x not in cols]])
    
def fc_load_slim(dbcode, pred_date = datetime.date(2019,1,21), 
                 exact = True, model = "90", pred_len = 15):
    """
    input: 품목 dbcode str & 전망시점 pred_date & 모델 코드 model, output: 레거시 모델 전망치 pd DataFrame
    """
    df = fc_load(dbcode, pred_date = pred_date, exact = exact)
    df = df[df["VIEW_CD"]==model].rename(columns = {"YEAR_SN":"year", "MONTH_SN":"month", "VIEW_PRICE":pred_date})
    df['day'] = 1
    df["Date"] = pd.to_datetime(df[['year','month','day']])
    df["Date"] = df["Date"].dt.date
    if exact:
        return(reset_date(set_date(df).sort_index()[[pred_date]].head(pred_len)))
    res_ = pd.DataFrame(columns = ["Date"])
    for pred_date_s in sorted(list(set(df['REG_DATE']))):
        temp = df[df['REG_DATE']==pred_date_s].copy().rename(columns = {pred_date:to_date(pred_date_s)})
        res_ = pd.merge(res_, temp[['Date', to_date(pred_date_s)]].head(pred_len), on = "Date", how = "outer")
    return(reset_date(round(set_date(res_).sort_index(), 3)))


# --------------------------------------------------
# 가격/통계 데이터 검색

def update_stats():
    userid, password = 'comm', 'koreapds'
    client = pymongo.MongoClient(f'mongodb://{userid}:{password}@192.168.0.124:27017/commodity')

    org_df = client['commodity']['dbcode_list'].find_one({"name":"모든 통계"})
    if to_datetime(datetime.date.today()) - relativedelta(days = 7) < org_df['reg_date']:
        return()
    client['commodity']['dbcode_list'].delete_one({"name":"모든 통계"})
    
    organ_df = query_basic("""SELECT * FROM COMM.TBL_COMM_STATS_ORGAN""", order = -1)
    organ_df['ORGAN_CD'] = organ_df['ORGAN_CD'].astype(int)
    
    stats_df = query_basic("""SELECT A.STATS_LARGE_CD, A.STATS_MIDDLE_CD,
    A.STATS_SMALL_CD, A.STATS_ITEM_CD, A.STATS_ITEMD_CD,
    A.ZONE_CD, A.NATION_CD, A.COMPANY_CD, A.ORGAN_CD, A.PERIOD_CD,
    A.UNITS, B.LAST_REG_DATE, B.SERVICE_NM, B.DIFF_TYPE
    FROM comm.TBL_COMM_STATS_ETL_SERVICE A, comm.TBL_COMM_STATS_SERVICE B
    WHERE A.STATS_LARGE_CD = B.STATS_LARGE_CD
    AND A.STATS_MIDDLE_CD = B.STATS_MIDDLE_CD
    AND A.STATS_SMALL_CD = B.STATS_SMALL_CD
    AND A.STATS_ITEM_CD = B.STATS_ITEM_CD
    AND A.STATS_ITEMD_CD = B.STATS_ITEMD_CD
    AND A.ZONE_CD = B.ZONE_CD
    AND A.NATION_CD = B.NATION_CD
    AND A.COMPANY_CD = B.COMPANY_CD
    AND A.ORGAN_CD = B.ORGAN_CD
    AND A.PERIOD_CD = B.PERIOD_CD""", order = -1)
    stats_df['ORGAN_CD'] = stats_df['ORGAN_CD'].astype(int)
    stats_df['dbcode'] = stats_df[list(stats_df)[:10]].apply(lambda x: "-".join([str(y) for y in x]), axis = 1)

    stats_df = pd.merge(stats_df, organ_df[["ORGAN_CD", "ORGAN_NM"]], on = 'ORGAN_CD', how = 'left')
    stats_df = stats_df.rename(columns = {"LARGE_NAME":"sector", "SERVICE_NM":"item_name", 
                                          "DETAIL_NAME":"item_spec", "DIFF_TYPE":"통계 타입"})
    stats_df = stats_df[~stats_df['item_name'].isna()][['item_name', 'dbcode'] + [x for x in list(stats_df) if x not in ['item_name', 'dbcode'] ]]
    org_df = pd.merge(df_comp(org_df).drop("LAST_REG_DATE", axis = 1), 
                      stats_df[['dbcode', 'LAST_REG_DATE']], on = "dbcode")
    
    rec_df = [{"dbcode":np.nan, "init_date":np.nan, "last_date":np.nan}]
    for dbcode in stats_df[~stats_df['dbcode'].isin(df_comp(org_df)['dbcode'])]['dbcode']:
        temp = pr_q_slim(dbcode)
        rec_df.append({"dbcode":dbcode, 
                       "init_date":temp["Date"].min(), "last_date":temp["Date"].max()})
    rec_df = pd.DataFrame(rec_df).dropna()
    if len(rec_df.index) != 0:
        rec_df['init_date'] = rec_df['init_date'].map(lambda x: to_datetime(x))
        rec_df['last_date'] = rec_df['last_date'].map(lambda x: to_datetime(x))
        org_df = pd.concat([org_df, pd.merge(stats_df, rec_df, on = 'dbcode')])
    client['commodity']['dbcode_list'].insert_one({"name":"모든 통계", "cols":list(org_df), 
                                                   "df":org_df.values.tolist(), 'author':'kk', 
                                                   'reg_date':to_datetime(datetime.date.today()),
                                                   "description":"모든 통계 데이터"})

def stat_search(key = " ", dict_form = False, 
                cols = ['item_name', 'dbcode', "UNITS", "ORGAN_NM"]):
    df, df_dict = get_items("모든 통계"), {}
    for keyword in key.split(" "):
        df = df[df['item_name'].str.contains(keyword)].sort_values(by = "PERIOD_CD")
    if dict_form:
        for idx in range(len(df.index)):
            df_dict[df.iloc[idx]['item_name']] = df.iloc[idx]['CODE']
        return(df_dict)
    if type(cols) == list:
        return(df[cols])
    else:
        return(df)

def price_search(key = " ", dict_form = False):
    df, df_dict = get_items("모든 가격"), {}
    for keyword in key.split(" "):
        df = df[df['item_name'].str.contains(keyword)]
    if dict_form:
        for idx in range(len(df.index)):
            df_dict[df.iloc[idx]['item_name']] = df.iloc[idx]['CODE']
        return(df_dict)
    return(df)


# --------------------------------------------------
# MongoDB 업로드를 위한 dataframe to dictionary of lists

def df_decomp(df_o, cols = ["Date"]):
    """
    input: pd DataFrame, output: MongoDB 업로드 가능한 dictionary of lists, ["df", "cols"]
    """
    pred_val = reset_date(df_o)
    for col in [x for x in cols if x in list(pred_val)]:
        pred_val[col] = pred_val[col].map(lambda x: to_datetime(x))
    return({'df':pred_val.values.tolist(),  "cols":[str(x) for x in list(pred_val)]})

def df_comp(df_dict):
    """
    input: MongoDB 업로드 가능한 dictionary of lists, ["df", "cols"], output: pd DataFrame
    """
    if 'df' in list(df_dict) and "cols" in list(df_dict):
        pred_val = pd.DataFrame(df_dict['df'], columns = df_dict['cols'])
        pred_val = datecol_dt(pred_val)
        if type(list(pred_val)[-1]) == datetime.datetime:
            pred_val = set_date(pred_val)
            pred_val.columns = [x.date() for x in list(pred_val)]
        return(reset_date(pred_val))
    elif type(df_dict) == pd.core.frame.DataFrame:
        return(df_dict.copy())
    else:
        print("dictionary에 df 또는 cols key가 존재하지 않습니다")
        sys.exit()
        

# --------------------------------------------------
# A.I. Forecasting용 함수

def best_score(df_o, cols = "Data", percent = True, w_t = 0, c_off = [1, 0.1, 0.05]):
    s_df = set_date(df_o).sort_index()
    s_df["w_t"] = pd.Series([ x ** w_t for x in range(1, len(s_df.index) + 1)], index = s_df.index)
    col, last_idx = df_cols(df_o, cols = cols)[0], s_df.index[-1]
    if percent:
        s_df = s_df.apply(lambda x: abs(x/x[col] - 1) * x["w_t"], axis = 1)
    else:
        s_df = s_df.apply(lambda x: abs(x - x[col]) * x["w_t"], axis = 1)
    s_df.loc['score']= s_df.mean()
    s_df = s_df.drop(["Data", "w_t"], axis = 1).T
    res_dict = {}
    for c_off_i in c_off:
        if c_off_i == 1:
            temp = s_df.sort_values("score").T
        else:
            temp = s_df[s_df[last_idx]< c_off_i * (len(s_df.index)  ** w_t) ].sort_values("score").T
        if len(list(temp)) != 0:
            res_dict[c_off_i] = temp.copy()
    return(res_dict)

# def hist_load(sample):
#     if type(sample['history']) == list:
#         h_df = pd.DataFrame(sample['history'], columns = sample['cols'])
#         h_df.columns = ['Date', 'Data'] + [datetime.datetime.strptime(x, "%Y-%m-%d").date() for x in list(h_df) if x not in ['Date', 'Data']]
#         h_df['Date'] = h_df['Date'].dt.date
#     else:
#         h_df = sample['history'].copy()
#     return(h_df)

def hist_err(hist_df, head_num = 12, summary = True):
    """
    input: history df, pd DataFrame 형식, output: 전망 성적 평균, pd DataFrame 형식
    """
    sc_df = reset_date(hist_df).reset_index(drop = True)
    cols = sorted([x for x in list(sc_df) if x not in ['Date', "Data"]])
    sc_df = set_date(sc_df[sc_df.index>=sc_df[cols[0]].first_valid_index()])[["Data"] + cols]
    sc_df = sc_df.apply(lambda x: abs(x/x['Data'] - 1)*100, axis = 1).reset_index().dropna(axis = 1, how = "all")

    for col in [x for x in list(sc_df) if x not in ['Date', "Data"]]:
        sc_df[col] = sc_df[col].shift(-sc_df[col].first_valid_index())
    sc_df = sc_df.drop(["Date","Data"], axis = 1).head(head_num).reset_index()
    sc_df['전망 평가 기준'] = sc_df['index'].map(lambda x: str(x +1) + "M Spot")
    if summary:
        sc_df = sc_df.dropna(axis = 1).reset_index()
        sc_df['평균 전망 성적'] = sc_df[[x for x in cols if x in list(sc_df)]].mean(axis = 1)
        return(round(sc_df[['전망 평가 기준', "평균 전망 성적"] + [x for x in cols if x in list(sc_df)]], 3))
    return(round(sc_df[['전망 평가 기준'] + [x for x in cols if x in list(sc_df)]], 3))

def closest_cols(df_o, val = "def"):
    df = set_date(df_o).iloc[:1].reset_index(drop = True)
    if type(val) not in [int, float]:
        print('closest_cols, 평균값을 기준으로 계산합니다')
        val = df.iloc[0].mean()
    df = df.apply(lambda x: abs(x/val - 1))
    return(set_date(df_o)[list(df.T.sort_values(0).index)])

# --------------------------------------------------
# 편의용 함수

import random, string

def rand_str(digit_n = 6):
    return(''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(digit_n)))

import statsmodels.formula.api as sm

def proj_data(df_o, col = "Data", how = "ols", prev = 6):
    df = add_yoy(reset_date(df_o), cols=col).reset_index()
    freq = data_freq(df)
    if how == 'ols':
        model = sm.ols(col+"_YoY ~ " + "index", data = df.iloc[-prev:]).fit()
        rate = model.predict(pd.DataFrame([{'index':df.iloc[-1]["index"] + 1}]))[0]
    else:
        rate = df.iloc[-prev:][col + "_YoY"].mean()
    temp_df = add_data(df[['Date', col]].dropna(), 1)
    val = df[df['Date'] == temp_df['Date'].max() - relativedelta(years = 1)][col].iloc[-1]
    return(add_data(df[['Date', col]].dropna(), round(val*(100 + rate)/100, 2)))

def month_eq(df_o, col = -1):
    df = set_date(df_o)
    max_t = df[[df_cols(df, cols = col)[0]]].dropna().index.max()
    df['month'] = df.index.map( lambda x: x.month)
    return(df[df['month']<=max_t.month].drop("month", axis = 1))

def std_ols(df_o, y = -1, diff = False, predict = False):
    df = set_date(df_o).dropna()
    if y == -1 or y not in list(df):
        y = df_cols(pic_df)[0]
    df = df[[y] + [x for x in list(df) if x != y]]
    col_dict, rev_dict, mean_std = {}, {}, {}
    for col in list(df):
        col_dict[col] = "x" + str(list(df).index(col))
        rev_dict["x" + str(list(df).index(col))] = col
        if diff:
            df[col] = df[col].diff(1)
        mean_std[col] = [df[col].mean(), df[col].std()]
        df[col] = (df[col] - df[col].mean())/df[col].std()
    df = df.rename(columns = col_dict)
    model = sm.ols(formula = "x0 ~ "+ " + ".join(list(df)[1:]), data = df).fit()
    param_df = pd.merge(pd.DataFrame(model.params, columns = ['coeff']).reset_index(),
                       pd.DataFrame(model.pvalues, columns = ['p-value']).reset_index(), on = "index")
    param_df["R2"] = model.rsquared
    param_df = round(param_df, 3)
    param_df['index'] = param_df['index'].map(rev_dict)
    if type(predict) == type(param_df):
        p_df = set_date(predict)
        for col in list(p_df):
            p_df[col] = (p_df[col] - mean_std[col][0])/mean_std[col][1]
        pred_val = pd.DataFrame(model.predict(p_df.rename(columns = col_dict)), columns = [y])
        pred_val[y] = pred_val[y] * mean_std[y][1] + mean_std[y][0]
        return(param_df.dropna().rename(columns = {'index':"독립 변수"}), 
              reset_date(pred_val))
    else:
        return(param_df.dropna().rename(columns = {'index':"독립 변수"}))
    

# def missing_df(df_o):
#     """
#     df내 가장 이른 기간 이후로 빈칸 없는 df 리턴. 이후는 datetime이 index화
#     """
#     df = reset_date(df_o)
#     return(df[df['Date']>=df.dropna()['Date'].iloc[0]].fillna(method='ffill').set_index("Date"))

# def dict_to_df(dict_samp):
#     """
#     input: , output: 
#     """
#     res_df = []
#     for key in list(dict_samp):
#         res_df.append({"key":key, "value":dict_samp[key]})
#     return(pd.DataFrame(res_df))

# def china_stat(df_o):
#     df = reset_date(df_o)
#     df.index = pd.to_datetime(df["Date"])
#     df = df.loc[str(df.index.min().year + 1):str(df.index.max().year - 1)].drop('Date', axis = 1)
#     if len(df[df.index.month == 1].index)> len(df[df.index.month == 2].index):
#         non_ext, exist_m = 2, 1
#     else:
#         non_ext, exist_m = 1, 2
#     miss_n = len(df[df.index.month == exist_m].index) - len(df[df.index.month == non_ext].index)
#     df = pd.merge(stre_date(df_o, freq = "MS"), df_o, on = "Date", how = 'left').sort_values("Date")
#     df.index = pd.to_datetime(df["Date"])
#     if len(df[(df.index.month == exist_m) &(df['Data'].shift(1) < df['Data']) 
#               & (df['Data'].shift(-1) < df['Data'] )].copy().index) >= miss_n:
#         df['Data'] = np.where((df.index.month == non_ext) & (df['Data'].isna()), 
#                               df["Data"].shift(non_ext - exist_m)/2, df["Data"])
#     else:
#         df['Data'] = np.where((df.index.month == non_ext) & (df['Data'].isna()), 
#                               df["Data"].shift(non_ext - exist_m), df["Data"])
#     return(df.drop("Date", axis = 1))
