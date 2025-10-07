【上市資料】
[個股成交日資訊]
https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20251003&stockNo=006201&response=json

[個股收盤價]
https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_AVG?date=20251004&stockNo=0050&response=json

[三大法人]
https://www.twse.com.tw/rwd/zh/fund/BFI82U?type=day&dayDate=20251003&weekDate=20250930&monthDate=20251004&response=json

[融資融券餘額]
https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date=20251003&selectType=MS&response=json

[注意股公告]
https://www.twse.com.tw/rwd/zh/announcement/notice?querytype=1&stockNo=&selectType=&sortKind=STKNO&response=json&startDate=20250801&endDate=20251007


https://www.twse.com.tw/rwd/zh/announcement/notice?querytype=1&stockNo=&selectType=
&startDate=20251003
&endDate=20251004
&sortKind=STKNO&response=json

=======
統整
1.加權指數
https://www.twse.com.tw/zh/indices/taiex/mi-5min-hist.html
[GET]https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?response=json
{"stat":"OK","title":"114年10月 發行量加權股價指數歷史資料","date":"20251003","fields":["日期","開盤指數","最高指數","最低指數","收盤指數"],"data":[["114/10/01","26,078.29","26,325.79","25,982.91","25,982.91"],["114/10/02","26,392.31","26,489.79","26,330.25","26,378.39"],["114/10/03","26,435.10","26,761.06","26,410.03","26,761.06"]],"total":3}

2.加權指數成交量
https://www.twse.com.tw/zh/trading/historical/fmtqik.html
[GET]https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?response=json
{"stat":"OK","date":"20251003","title":"114年10月市場成交資訊","hints":"單位：元、股","fields":["日期","成交股數","成交金額","成交筆數","發行量加權股價指數","漲跌點數"],"data":[["114/10/01","7,679,791,251","468,840,061,902","3,129,297","25,982.91","162.37"],["114/10/02","8,085,314,896","506,683,446,386","3,240,734","26,378.39","395.48"],["114/10/03","7,784,825,692","480,095,576,070","3,218,063","26,761.06","382.67"]],"notes":["當日統計資訊含大盤、零股、盤後定價及鉅額交易，不含拍賣、標購。","外幣成交值係以本公司當日下午3時30分公告匯率換算後加入成交金額。公告匯率請參考本公司首頁>產品與服務>交易系統>雙幣ETF專區>代號對應及每日公告匯率。"]}

3.加權指數三大法人
[三大法人]
https://www.twse.com.tw/rwd/zh/fund/BFI82U?type=day&dayDate=20251003&weekDate=20250930&monthDate=20251004&response=json

4.加權指數融資餘額
[融資融券餘額]
https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date=20251003&selectType=MS&response=json

******************

1.櫃買指數&成交量
https://www.tpex.org.tw/zh-tw/mainboard/trading/info/daily-indices.html
[method]POST
[url]https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingIndex
[Payload-formData]date=2025%2F10%2F01&id=&response=json


2.櫃買指數三大法人
https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/summary/day.html
[method]POST
[url]https://www.tpex.org.tw/www/zh-tw/insti/summary
[Payload-formData]type=Daily&prod=1&date=&id=&response=json


3.櫃買指數融資餘額
https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/transactions.html
[method]POST
[url]https://www.tpex.org.tw/www/zh-tw/margin/balance
[Payload-formData]date=&id=&response=json

4.櫃買 注意股公告
https://www.tpex.org.tw/zh-tw/announce/market/attention.html
[method]POST
[url]https://www.tpex.org.tw/www/zh-tw/bulletin/attention
[Payload-formData]startDate=2025%2F01%2F01&endDate=2025%2F10%2F07&code=&cate=&type=all&order=date&id=&response=json