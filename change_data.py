import os
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from time import sleep

# 國家代碼
country = {
    '阿富汗':'AF',
    '阿爾巴尼亞':'AL',
    '阿爾及利亞':'DZ',
    '美屬薩摩亞':'AS',
    '安道爾':'AD',
    '安哥拉':'AO',
    '英屬安圭拉':'AI',
    '南極洲':'AQ',
    '安地卡及巴布達':'AG',
    '阿根廷':'AR',
    '亞美尼亞':'AM',
    '阿魯巴':'AW',
    '澳大利亞':'AU',
    '奧地利':'AT',
    '亞塞拜然':'AZ',
    '巴哈馬':'BS',
    '巴林':'BH',
    '孟加拉':'BD',
    '巴貝多':'BB',
    '白俄羅斯':'BY',
    '比利時':'BE',
    '貝里斯':'BZ',
    '貝南':'BJ',
    '百慕達':'BM',
    '不丹':'BT',
    '玻利維亞':'BO',
    '波士尼亞':'BA',
    '波扎那':'BW',
    '波維特島':'BV',
    '巴西':'BR',
    '英屬印度洋地區':'IO',
    '汶萊':'BN',
    '保加利亞':'BG',
    '上伏塔':'BF',
    '蒲隆地':'BI',
    '高棉':'KH',
    '喀麥隆':'CM',
    '加拿大':'CA',
    '佛德角':'CV',
    '開曼群島':'KY',
    '中非共和國':'CF',
    '查德':'TD',
    '智利':'CL',
    '中國大陸':'CN',
    '聖誕島':'CX',
    '可可斯群島':'CC',
    '哥倫比亞':'CO',
    '葛摩':'KM',
    '剛果':'CG',
    '薩伊':'CD',
    '科克群島':'CK',
    '哥斯大黎加':'CR',
    '象牙海岸':'CI',
    '克羅埃西亞':'HR',
    '古巴':'CU',
    '塞普路斯':'CY',
    '捷克':'CZ',
    '丹麥':'DK',
    '吉布地':'DJ',
    '多米尼克':'DM',
    '多明尼加':'DO',
    '帝汶':'TP',
    '厄瓜多爾':'EC',
    '埃及':'EG',
    '薩爾瓦多':'SV',
    '赤道幾內亞':'GQ',
    '厄利垂亞':'ER',
    '愛沙尼亞':'EE',
    '衣索比亞':'ET',
    '法羅群島':'FO',
    '福克蘭群島':'FK',
    '斐濟':'FJ',
    '芬蘭':'FI',
    '法國':'FR',
    '法屬圭亞那':'GF',
    '法屬玻里尼西亞':'PF',
    '法屬南部屬地':'TF',
    '加彭':'GA',
    '甘比亞':'GM',
    '喬治亞':'GE',
    '德國':'DE',
    '迦納':'GH',
    '直布羅陀':'GI',
    '希臘':'GR',
    '格陵蘭':'GL',
    '格瑞那達':'GD',
    '瓜德魯普島':'GP',
    '關島':'GU',
    '瓜地馬拉':'GT',
    '根息島':'GG',
    '幾內亞':'GN',
    '幾內亞比索':'GW',
    '蓋亞那':'GY',
    '海地':'HT',
    '赫德及麥當勞群島':'HM',
    '教廷':'VA',
    '宏都拉斯':'HN',
    '香港':'HK',
    '匈牙利':'HU',
    '冰島':'IS',
    '印度':'IN',
    '印尼':'ID',
    '伊朗':'IR',
    '伊拉克':'IQ',
    '愛爾蘭':'IE',
    '英屬曼島':'IM',
    '以色列':'IL',
    '義大利':'IT',
    '牙買加':'JM',
    '日本':'JP',
    '澤西島':'JE',
    '約旦':'JO',
    '哈薩克':'KZ',
    '肯亞':'KE',
    '吉里巴斯':'KI',
    '北韓':'KP',
    '大韓民國':'KR',
    '科威特':'KW',
    '吉爾吉斯':'KG',
    '寮國':'LA',
    '拉脫維亞':'LV',
    '黎巴嫩':'LB',
    '賴索托':'LS',
    '賴比瑞亞':'LR',
    '利比亞':'LY',
    '列支敦斯堡':'LI',
    '立陶宛':'LT',
    '盧森堡':'LU',
    '澳門':'MO',
    '馬其頓':'MK',
    '馬達加斯加':'MG',
    '馬拉威':'MW',
    '馬來西亞':'MY',
    '馬爾地夫':'MV',
    '馬利':'ML',
    '馬爾他':'MT',
    '馬紹爾群島':'MH',
    '法屬馬丁尼克':'MQ',
    '茅利塔尼亞':'MR',
    '模里西斯':'MU',
    '美亞特':'YT',
    '墨西哥':'MX',
    '密克羅尼西亞':'FM',
    '摩爾多瓦':'MD',
    '摩納哥':'MC',
    '蒙古':'MN',
    '蒙瑟拉特島':'MS',
    '摩洛哥':'MA',
    '莫三鼻給':'MZ',
    '緬甸':'MM',
    '納米比亞':'NA',
    '諾魯':'NR',
    '尼伯爾':'NP',
    '荷屬安地列斯':'AN',
    '荷蘭':'NL',
    '新喀里多尼亞':'NC',
    '紐西蘭':'NZ',
    '尼加拉瓜':'NI',
    '尼日':'NE',
    '奈及利亞':'NG',
    '紐威島':'NU',
    '諾福克群島':'NF',
    '北里亞納群島':'MP',
    '挪威':'NO',
    '阿曼':'OM',
    '巴基斯坦':'PK',
    '帛琉':'PW',
    '巴拿馬':'PA',
    '巴拿馬運河區':'PZ',
    '巴布亞新幾內亞':'PG',
    '巴拉圭':'PY',
    '秘魯':'PE',
    '菲律賓':'PH',
    '皮特康島':'PN',
    '波蘭':'PL',
    '葡萄牙':'PT',
    '波多黎各':'PR',
    '庫達':'QA',
    '留尼旺':'RE',
    '羅馬尼亞':'RO',
    '俄羅斯聯邦':'RU',
    '盧安達':'RW',
    '聖赫勒拿島':'SH',
    '聖克里斯多福':'KN',
    '聖露西亞':'LC',
    '聖匹及密啟倫群島':'PM',
    '聖文森':'VC',
    '薩摩亞獨立國':'WS',
    '聖馬利諾':'SM',
    '聖托馬－普林斯浦':'ST',
    '沙烏地阿拉伯':'SA',
    '塞內加爾':'SN',
    '塞爾維亞與蒙特尼哥羅':'CS',
    '塞席爾共和國':'SC',
    '獅子山':'SL',
    '新加坡':'SG',
    '斯洛伐克':'SK',
    '斯洛凡尼亞':'SI',
    '索羅門群島':'SB',
    '索馬利亞':'SO',
    '南非共和國':'ZA',
    '南三明治群島':'GS',
    '西班牙':'ES',
    '斯里蘭卡':'LK',
    '蘇丹':'SD',
    '蘇利南':'SR',
    '斯瓦巴及尖棉島':'SJ',
    '史瓦濟蘭':'SZ',
    '瑞典':'SE',
    '瑞士':'CH',
    '敘利亞':'SY',
    '中華民國':'TW',
    '塔吉克':'TJ',
    '坦尚尼亞':'TZ',
    '泰國':'TH',
    '多哥':'TG',
    '托克勞群島':'TK',
    '東加':'TO',
    '千里達－托貝哥':'TT',
    '突尼西亞':'TN',
    '土耳其':'TR',
    '土庫曼':'TM',
    '土克斯及開科斯群島':'TC',
    '吐瓦魯':'TV',
    '烏干達':'UG',
    '烏克蘭':'UA',
    '阿拉伯聯合大公國':'AE',
    '英國':'GB',
    '美國':'US',
    '美屬邊疆群島':'UM',
    '烏拉圭':'UY',
    '烏玆別克':'UZ',
    '萬那杜':'VU',
    '委內瑞拉':'VE',
    '越南':'VN',
    '英屬維京群島':'VG',
    '美屬維京群島':'VI',
    '沃里斯及伏塔那島':'WF',
    '西撒哈拉':'EH',
    '北葉門':'YE',
    '尚比亞':'ZM',
    '辛巴威':'ZW',
    '其他亞洲國家':'XA',
    '其他歐洲國家':'XE',
    '其他美洲國家':'XM',
    '其餘國家':'XO',
    '帛琉群島':'PLW',
    '斐濟群島':'FJI',
    '其他國家':''
}
# 水果英文名稱
variety_dict = {
    '香蕉':'banana',
    '鳳梨':'pineapple',
    '芒果':'mango'
}
# 進出口代號
Import_and_export = {
    '進口':1,
    '出口':2
}

position = os.path.join('.', 'Reptile_Data')

def change_data():
    files = os.listdir(position) # 讀取資料夾所有檔案
    csv_files = []
    for f in files:
        if f.endswith('.csv'): # 篩選csv檔
            csv_files.append(f)

    for f in csv_files:
        fruit_name = f[-9:-7] # 水果名
        df = pd.read_csv(position+'/'+f, encoding='big5', index_col=False)
        df = df[df['Name']==fruit_name] # 篩選掉重複的資料
        df.loc[:,'Name'] = variety_dict[fruit_name] # 水果名轉成英文名
        for i in range(df.shape[0]):
            if df.iat[i,3] in country.keys():
                df.iat[i,3] = country[df.iat[i,3]] # 將國家名稱轉為代碼
            else:
                df.iat[i,3] = ''

        df = df[df.Country!=''] # 排除['Name']=其他國家
        df.reset_index(drop=True,inplace=True)# 重設index 因為前面有刪掉行所以原本的index有缺 ｜ drop=True會刪掉原本的index，不然reset_index()後會有兩個index
        df.loc[:,'Import and export'] = Import_and_export[df.loc[1,'Import and export']] # 將進出口轉為代號
        
        sleep(3)
        try:
            save_data(df,f)
            print('{}的資料已整理完成並儲存在 MySQL & csv檔'.format(f))
        except Exception as err:
            print(err)

def save_data(data,fname):
    '''
    Store information to csv & MySQL
    '''
    position_data = os.path.join('.','Data')
    #csv
    data.to_csv(position_data+'\\'+fname, encoding='big5', index=False)
    #MySQL
    pymysql.install_as_MySQLdb()
    engine = create_engine('mysql://root:Tcfst123456!@localhost/farm_data')
    data.to_sql(name=fname, con=engine, if_exists='replace', index=False)

if __name__ == '__main__':
    change_data()

