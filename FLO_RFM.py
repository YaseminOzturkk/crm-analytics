
###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranış öbeklenmelerine göre gruplar oluşturulacak..

###############################################################
# Veri Seti Hikayesi
###############################################################

# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan müşterilerin geçmiş alışveriş davranışlarından
# elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# GÖREVLER
###############################################################

#############################################################
# GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama
#############################################################

# 1. flo_data_20K.csv verisini okuyunuz.
import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
df_ = pd.read_csv("C:/Users/yasmi/Desktop/FLOMusteriSegmentasyonu/flo_data_20k.csv")
df = df_.copy()

# 2. Veri setinde
# a. İlk 10 gözlem,
df.head(10)
# b. Değişken isimleri,
df.columns
# c. Betimsel istatistik,
df.describe().T
# d. Boş değer,
df.isnull().sum()

# e. Değişken tipleri, incelemesi yapınız.
df.dtypes


# 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Herbir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturun.


df["order_num_total_ever_omnichannel"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["customer_value_total_ever_omnichannel"] = (df["customer_value_total_ever_online"]
                                               + df["customer_value_total_ever_offline"])
df.head()

# 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
df.dtypes
df["first_order_date"] = df["first_order_date"].astype('datetime64')
df["last_order_date"] = df["last_order_date"].astype('datetime64')
df["last_order_date_online"] = df["last_order_date_online"].astype('datetime64')
df["last_order_date_offline"] = df["last_order_date_offline"].astype('datetime64')


# 5. Alışveriş kanallarındaki müşteri sayısının, ortalama alınan ürün sayısının ve ortalama harcamaların dağılımına bakınız.


df.groupby("order_channel").agg({"master_id": "count",
                                 "order_num_total_ever_omnichannel": "mean",
                                 "customer_value_total_ever_omnichannel": "mean"})

# 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
df.head()
df.groupby("master_id").agg({"customer_value_total_ever_omnichannel": "sum"}).sort_values("customer_value_total_ever_omnichannel", ascending=False).head(10)

# 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
df.groupby("master_id").agg({"order_num_total_ever_omnichannel": "sum"}).sort_values("order_num_total_ever_omnichannel", ascending=False).head(10)


# 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.

def data_prepartion(df, csv=False):
    df["order_num_total_ever_omnichannel"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["customer_value_total_ever_omnichannel"] = (df["customer_value_total_ever_online"]
                                                   + df["customer_value_total_ever_offline"])
    df["first_order_date"] = df["first_order_date"].astype('datetime64')
    df["last_order_date"] = df["last_order_date"].astype('datetime64')
    df["last_order_date_online"] = df["last_order_date_online"].astype('datetime64')
    df["last_order_date_offline"] = df["last_order_date_offline"].astype('datetime64')


# GÖREV 2: RFM Metriklerinin Hesaplanması

# Recency, Frequency, Monetary
df.head()

df["last_order_date"].max()
today_date = dt.datetime(2021, 6, 1)
type(today_date)



rfm = df.groupby("master_id").agg({"last_order_date": lambda last_order_date: (today_date - last_order_date.max()).days,
                                   "order_num_total_ever_omnichannel":
                                       lambda order_num_total_ever_omnichannel: order_num_total_ever_omnichannel.sum(),
                                   "customer_value_total_ever_omnichannel":
                                       lambda customer_value_total_ever_omnichannel: customer_value_total_ever_omnichannel.sum()}).head()
rfm.columns = ["recency", "frequency", "monetary"]
rfm.head()
rfm.describe().T

# GÖREV 3: RF ve RFM Skorlarının Hesaplanması

rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])
rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) + rfm['frequency_score'].astype(str))
rfm.head()
# GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True) # scorları birlestirme


# GÖREV 5: Aksiyon zamanı!
# 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.

rfm["segment"].nunique()
rfm["segment"].unique()

rfm.groupby("segment").agg({"recency": "mean",
                            "frequency": "mean",
                            "monetary": "mean"})

# 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv ye kaydediniz.

# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri
# tercihlerinin üstünde. Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel
# olarak iletişime geçeilmek isteniliyor. Sadık müşterilerinden(champions,loyal_customers),
# ortalama 250 TL üzeri ve kadın kategorisinden alışveriş yapan kişiler özel olarak iletişim kuralacak müşteriler.
# Bu müşterilerin id numaralarını csv dosyasına yeni_marka_hedef_müşteri_id.cvs olarak kaydediniz.


target_customers = pd.DataFrame
# "segment" sütunu yalnızca "champions" veya "loyal_customers" olan müşterileri seçelim
target_customers = rfm[(rfm["segment"] == "champions")
                       | (rfm["segment"] == "loyal_customers")]

# Monetary değeri 250 TL üzeri olanları seçelim
target_customers = target_customers[target_customers["monetary"] > 250]

# Kadın ayakkabı kategorisini tercih edenleri seçelim
target_customers = df[df["interested_in_categories_12"].str.contains("KADIN", case=False, na=False)]

# Sadece müşteri ID sütununu seçelim
target_customer_ids = target_customers.index
target_customer_ids = target_customer_ids.to_frame()

# Sonuçları bir CSV dosyasına kaydedelim
target_customer_ids.to_csv("yeni_marka_hedef_müşteri_id.csv", header=["customer_id"], index=False)

# b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen
# geçmişte iyi müşteri olan ama uzun süredir alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve
# yeni gelen müşteriler özel olarak hedef alınmak isteniliyor. Uygun profildeki müşterilerin id'lerini csv dosyasına
# indirim_hedef_müşteri_ids.csv olarak kaydediniz.
target_customers2 = pd.DataFrame
# "segment" sütunu yalnızca "champions" veya "loyal_customers" olan müşterileri seçelim
target_customers2 = rfm[(rfm["segment"] == "hibernating")
                       | (rfm["segment"] == "about_to_sleep")
                       | (rfm["segment"] == "new_customers")]


target_customer2_ids = target_customers2.index
target_customer2_ids = target_customer2_ids.to_frame()
target_customer2_ids.to_csv("indirim_hedef_müşteri_id.csv", header=["customer_id"], index=False)
