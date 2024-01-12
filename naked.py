# Importing necessary libraries/modules
import requests # Lai veiktu HTTP pieprasījumus
import json # Darbam ar JSON datiem
import datetime # Datuma un laika apstrādei
import time # Laika funkcijām
import yaml # Darbam ar YAML datiem

from datetime import datetime # 'datetime' klases importēšana no 'datetime' moduļa
# Ziņojums par programmas sākumu
print('Asteroid processing service')

print('Loading configuration from file')

# NASA tuvāko Zemes objektu tīmekļa servisa API atslēga un URL
nasa_api_key = "dXvQFkmw5w8vxLbT0cGYEoLuoswdGyCnMPA43Gfv"
nasa_api_url = "https://api.nasa.gov/neo/"

# Šodienas datuma iegūšana
dt = datetime.now()
request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
print("Generated today's date: " + str(request_date))

# API pieprasījuma URL izvadīšana
print("Request url: " + str(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key))
# Pieprasījums NASA API
r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)

# Informācijas izvadīšana par API atbildi
print("Response status code: " + str(r.status_code))
print("Response headers: " + str(r.headers))
print("Response content: " + str(r.text))

# Pārbauda, vai API pieprasījums bija veiksmīgs (statusa kods 200)
if r.status_code == 200:
	# JSON datu izpilde no API atbildes
	json_data = json.loads(r.text)

	ast_safe = []  # Saraksts, lai uzglabātu nekaitīgus asteroidus
	ast_hazardous = [] # Saraksts, lai uzglabātu bīstamus asteroidus
	
	# Pārbauda, vai JSON datiem ir 'element_count'
	if 'element_count' in json_data:
		ast_count = int(json_data['element_count'])
		print("Asteroid count today: " + str(ast_count))
		 # Pārbauda, vai šodien ir kādi asteroidi
		if ast_count > 0:
			# Iterācija cauri asteroidu sarakstam
			for val in json_data['near_earth_objects'][request_date]:
				# Pārbauda, vai nepieciešamā informācija ir pieejama asteroida datiem
				if 'name' and 'nasa_jpl_url' and 'estimated_diameter' and 'is_potentially_hazardous_asteroid' and 'close_approach_data' in val:
					# Izgūst asteroida informāciju
					tmp_ast_name = val['name']
					tmp_ast_nasa_jpl_url = val['nasa_jpl_url']
					# Izgūst aptuveno diametra informāciju
					if 'kilometers' in val['estimated_diameter']:
						if 'estimated_diameter_min' and 'estimated_diameter_max' in val['estimated_diameter']['kilometers']:
							tmp_ast_diam_min = round(val['estimated_diameter']['kilometers']['estimated_diameter_min'], 3)
							tmp_ast_diam_max = round(val['estimated_diameter']['kilometers']['estimated_diameter_max'], 3)
						else:
							tmp_ast_diam_min = -2
							tmp_ast_diam_max = -2
					else:
						tmp_ast_diam_min = -1
						tmp_ast_diam_max = -1
						
					# Izgūst bīstamības informāciju
					tmp_ast_hazardous = val['is_potentially_hazardous_asteroid']
					
					# Pārbauda, vai asteroidam ir tuvākais pieejas dati
					if len(val['close_approach_data']) > 0:
						# Izgūst tuvākā pieejas informāciju
						if 'epoch_date_close_approach' and 'relative_velocity' and 'miss_distance' in val['close_approach_data'][0]:
							tmp_ast_close_appr_ts = int(val['close_approach_data'][0]['epoch_date_close_approach']/1000)
							tmp_ast_close_appr_dt_utc = datetime.utcfromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')
							tmp_ast_close_appr_dt = datetime.fromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')
							
							# Izgūst ātruma un izvairīšanās attāluma informāciju
							if 'kilometers_per_hour' in val['close_approach_data'][0]['relative_velocity']:
								tmp_ast_speed = int(float(val['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']))
							else:
								tmp_ast_speed = -1

							if 'kilometers' in val['close_approach_data'][0]['miss_distance']:
								tmp_ast_miss_dist = round(float(val['close_approach_data'][0]['miss_distance']['kilometers']), 3)
							else:
								tmp_ast_miss_dist = -1
						else:
							tmp_ast_close_appr_ts = -1
							tmp_ast_close_appr_dt_utc = "1969-12-31 23:59:59"
							tmp_ast_close_appr_dt = "1969-12-31 23:59:59"
					else:
						print("No close approach data in message")
						tmp_ast_close_appr_ts = 0
						tmp_ast_close_appr_dt_utc = "1970-01-01 00:00:00"
						tmp_ast_close_appr_dt = "1970-01-01 00:00:00"
						tmp_ast_speed = -1
						tmp_ast_miss_dist = -1
					# Izvadīt asteroida informāciju

					print("------------------------------------------------------- >>")
					print("Asteroid name: " + str(tmp_ast_name) + " | INFO: " + str(tmp_ast_nasa_jpl_url) + " | Diameter: " + str(tmp_ast_diam_min) + " - " + str(tmp_ast_diam_max) + " km | Hazardous: " + str(tmp_ast_hazardous))
					print("Close approach TS: " + str(tmp_ast_close_appr_ts) + " | Date/time UTC TZ: " + str(tmp_ast_close_appr_dt_utc) + " | Local TZ: " + str(tmp_ast_close_appr_dt))
					print("Speed: " + str(tmp_ast_speed) + " km/h" + " | MISS distance: " + str(tmp_ast_miss_dist) + " km")
					
					# Pievienot asteroida datus atbilstošajam sarakstam
					if tmp_ast_hazardous == True:
						ast_hazardous.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])
					else:
						ast_safe.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])

		else:
			print("No asteroids are going to hit earth today")

	print("Hazardous asteorids: " + str(len(ast_hazardous)) + " | Safe asteroids: " + str(len(ast_safe)))
	# Bīstamo asteroīdu kārtošana pēc tuvākā pieejas laika zīmes
	if len(ast_hazardous) > 0:

		ast_hazardous.sort(key = lambda x: x[4], reverse=False)

		print("Today's possible apocalypse (asteroid impact on earth) times:")
		 # Informācijas izvadīšana par bīstamiem asteroīdiem
		for asteroid in ast_hazardous:
			print(str(asteroid[6]) + " " + str(asteroid[0]) + " " + " | more info: " + str(asteroid[1]))

		ast_hazardous.sort(key = lambda x: x[8], reverse=False)
		 # Informācijas izvadīšana par bīstamo asteroīdu ar tuvāko pieeju attālumam
		print("Closest passing distance is for: " + str(ast_hazardous[0][0]) + " at: " + str(int(ast_hazardous[0][8])) + " km | more info: " + str(ast_hazardous[0][1]))
	else:
		print("No asteroids close passing earth today")

else:
	print("Unable to get response from API. Response code: " + str(r.status_code) + " | content: " + str(r.text))
