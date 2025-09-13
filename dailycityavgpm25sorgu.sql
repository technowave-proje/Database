-- Şehir bazında günlük ortalama PM2.5

SELECT l.city_name,
       DATE(a.timestamp) AS measurement_date,
       AVG(a.pm25) AS avg_pm25
FROM AirQualityData a
JOIN Locations l ON a.location_id = l.location_id
GROUP BY l.city_name, DATE(a.timestamp);

-- HAVING ile filtre (ör. ortalama 35 üstü günler)
SELECT l.city_name,
       DATE(a.timestamp) AS measurement_date,
       AVG(a.pm25) AS avg_pm25
FROM AirQualityData a
JOIN Locations l ON a.location_id = l.location_id
GROUP BY l.city_name, DATE(a.timestamp)
HAVING AVG(a.pm25) > 35;

-- Sık kullanılan sorguyu VIEW olarak kaydet

CREATE VIEW DailyCityAvgPM25 AS
SELECT l.city_name,
       DATE(a.timestamp) AS measurement_date,
       AVG(a.pm25) AS avg_pm25
FROM AirQualityData a
JOIN Locations l ON a.location_id = l.location_id
GROUP BY l.city_name, DATE(a.timestamp);

--Artık her defasında uzun sorgu yazmana gerek yok. Kullanımı:
--SELECT * FROM DailyCityAvgPM25 WHERE avg_pm25 > 50;

-- Bu sorgu şehir bazında günlük ortalama PM2.5 değerlerini hesaplar
-- AirQualityData (ölçümler) ile Locations (şehir bilgileri) tablolarını JOIN yapıyoruz
CREATE VIEW DailyCityAvgPM25 AS
SELECT l.city_name,
       DATE(a.timestamp) AS measurement_date,
       AVG(a.pm25) AS avg_pm25
FROM AirQualityData a
JOIN Locations l ON a.location_id = l.location_id
GROUP BY l.city_name, DATE(a.timestamp);
