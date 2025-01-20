import json
import sys
from pyspark.sql import SparkSession
from src.usva_app import VehicleAccAnalysisUS

if __name__ == "__main__":

    # Initialize the Spark Session
    spark = SparkSession.builder.appName("USVehicleAccidentsAnalysisApp").getOrCreate()

    config_path = sys.argv[1]

    # Read config file
    with open(config_path, 'r') as f:
        config_file = json.load(f)

    spark.sparkContext.setLogLevel("ERROR")

    # Setup output file path and format
    output_file_path = config_file.get("OUTPUT_PATH")

    file_format = config_file.get("FILE_FORMAT")

    vaa_us = VehicleAccAnalysisUS(spark, config_file)

    # 1. Find the number of crashes (accidents) in which number of males killed are greater than 2?
    print("1. Output: ", vaa_us.count_accidents(output_file_path.get('1'), file_format.get("Output")))

    # 2. How many two wheelers are booked for crashes? 
    print("2. Output: ", vaa_us.count_two_wheelers(output_file_path.get('2'), file_format.get("Output")))

    # 3. Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
    print("3. Output: ", vaa_us.top5_vehicle_makers(output_file_path.get('3'), file_format.get("Output")))

    # 4. Determine number of Vehicles with driver having valid licences involved in hit and run? 
    print("4. Output: ", vaa_us.hit_and_run(output_file_path.get('4'), file_format.get("Output")))

    # 5. Which state has highest number of accidents in which females are not involved? 
    print("5. Output: ", vaa_us.acc_state_with_no_female(output_file_path.get('5'), file_format.get("Output")))

    # 6. Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death 
    print("6. Output: ", vaa_us.top_vehicle_makers_contr_injuries(output_file_path.get('6'), file_format.get("Output")))

    # 7. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
    vaa_us.top_ethnic_user_grp_unique_style(output_file_path.get('7'), file_format.get("Output"))

    # 8. Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code) 
    print("8. Output: ", vaa_us.top5_zip_codes(output_file_path.get('8'), file_format.get("Output")))

    # 9. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    print("9. Output: ", vaa_us.crash_ids_with_nodamage(output_file_path.get('9'), file_format.get("Output")))

    # 10. Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences
    print("10. Output: ", vaa_us.top5_vehicle_makers_speeding_offence(output_file_path.get('10'), file_format.get("Output")))
    
    spark.stop()




        