from src.utils import data_loader
from pyspark.sql.functions import col, upper, row_number
from pyspark.sql import Window

class VehicleAccAnalysisUS:

    def __init__(self):

        pass

    def __init__(self, spark, config):

        # Get the input file path from config file
        input_file_path = config.get("INPUT_FILENAME")
        
        # Initialize dataframe, this df is scalable it will read new csv files from data dir
        self.df = data_loader(spark, input_file_path)
        
        """
        self.df_damages = data_loader(spark, input_file_path.get("Damages"))
        self.df_endorse = data_loader(spark, input_file_path.get("Endorse"))
        self.df_primary_person = data_loader(spark, input_file_path.get("Primary_Person"))
        self.df_restrict = data_loader(spark, input_file_path.get("Restrict"))
        self.df_units = data_loader(spark, input_file_path.get("Units"))
        """

    # Analysis 1
    def count_accidents(self, output_path, format):

        """
        Find the number of crashes (accidents) in which number of males killed
        Parameters:
            - output_path (str) : Output File path
            - format (str) : Output File format
        Returns:
            - (int) : Count of Accidents with Males 
        """
        try:

            male_df = self.df['Primary_Person_use'].filter(self.df['Primary_Person_use'].PRSN_GNDR_ID == "MALE")
            #print("Male count:", male_df.count())

            death_df = self.df['Primary_Person_use'].filter(self.df['Primary_Person_use'].DEATH_CNT == 1)
            #print("Death count: ", death_df.count())

            # df = self.df['Primary_Person_use'].filter(
            #     self.df['Primary_Person_use'].PRSN_GNDR_ID == "MALE"
            # ).filter(self.df['Primary_Person_use'].DEATH_CNT == 1)

            df = self.df['Primary_Person_use'].filter("PRSN_GNDR_ID =='MALE' and  DEATH_CNT==1").select('CRASH_ID')

            return df.count()

        except Exception as exception:
            print('Error::{}'.format(exception))

    # Analysis 2
    def count_two_wheelers(self, output_path, format):
        """
        How many two wheelers are booked for crashes? 
        Parameters:
            - output_path (str) : Output File path
            - format (str) : Output File format
        Returns:
            - (int) : Count of two wheelers in crashes
        """
        try:
            res_df = self.df['Units_use'].filter(upper(col('VEH_BODY_STYL_ID')).like('%MOTORCYCLE%'))

            return res_df.count()
        
        except Exception as exception:
            print('Error::{}'.format(exception))

    # Analysis 3
    def top5_vehicle_makers(self, output_path, format):
        """
        Determine the Top 5 Vehicle Makers of the cars present in the crashes in which driver died and Airbags did not deploy.
        Parameters:
            - output_path (str) : Output File path
            - format (str) : Output File format
        Returns:
            - (list) : top 5 Vehicle Makers of cars
        """
        try:
            res_df = (self.df['Units_use'].join(self.df['Primary_Person_use'], on="CRASH_ID", how="inner")).filter(
                      (col('PRSN_INJRY_SEV_ID') == 'KILLED')
                      & (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED')
                      & (col("VEH_MAKE_ID") != 'NA')
                 ).groupby('VEH_MAKE_ID').count().orderBy(col('count').desc()).limit(5)
        
            return [row[0] for row in res_df.collect()]
        
        except Exception as exception:
            print('Error::{}'.format(exception))

    # Analysis 4
    def hit_and_run(self, output_path, format):
        """
        Determine number of Vehicles with driver having valid licences involved in hit and run?
        Parameters:
            - output_path (str) : Output File path
            - format (str) : Output File format
        Returns:
            - (int) : Count of vehicles in hit & run
        """
        try:
            res_df = (self.df['Units_use'].select("CRASH_ID", "VEH_HNR_FL").join(self.df['Primary_Person_use'].select("CRASH_ID", "DRVR_LIC_TYPE_ID"), 
                    on="CRASH_ID", 
                    how="inner")
                 ).filter(
                    (col('DRVR_LIC_TYPE_ID')).isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.'])
                    & (col('VEH_HNR_FL') == "Y")
                 )
        except Exception as exception:
            print('Error::{}'.format(exception))

        return res_df.count()

    # Analysis 5
    def acc_state_with_no_female(self, output_path, format):
        """
        Which state has highest number of accidents in which females are not involved? 
        Parameters:
            - output_path (str) : Output File path
            - format (str) : Output File format
        Returns:
            - (str) : State Name
        """
        try:
            res_df = (
            self.df['Primary_Person_use'].filter(col('PRSN_GNDR_ID') != 'FEMALE').groupby('DRVR_LIC_STATE_ID').count().orderBy(col('count').desc())
            )

        except Exception as exception:

            print('Error::{}'.format(exception))

        #print("Res First", res_df.first())
        return res_df.first().DRVR_LIC_STATE_ID

    # Analysis 6
    def top_vehicle_makers_contr_injuries(self, output_path, format):

        """
        Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death 
        Parameters:
            - output_path (str) : Output File path
            - format (str) : Output File format
        Returns:
            - (list) : Top 3rd to 5th VEH_MAKE_IDs
        """
        try:
            top_df = (self.df['Units_use'].filter(col('VEH_MAKE_ID') != "NA")
                  .withColumn('TOT_CASUALTIES_CNT', self.df['Units_use'].TOT_INJRY_CNT + self.df['Units_use'].DEATH_CNT)
                  .groupby("VEH_MAKE_ID") 
                  .sum('TOT_CASUALTIES_CNT')
                  .withColumnRenamed("sum(TOT_CASUALTIES_CNT)", 'TOT_CASUALTIES_CNT_AGG')
                  .orderBy(col('TOT_CASUALTIES_CNT_AGG').desc())
                )
            
        except Exception as exception:

            print('Error::{}'.format(exception))

        res_df = top_df.limit(5).subtract(top_df.limit(2))

        return [veh[0] for veh in res_df.select('VEH_MAKE_ID').collect()]

    # Analysis 7
    def top_ethnic_user_grp_unique_style(self, output_path, format):
        """
        For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
        Parameters:
            - output_path (str) : Output File path
            - format (str) : Output File format
        Output:
            - (df) : Top ethnic user group of each unique body style
        """
        try:
            window = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col('count').desc())

            res_df = (
                self.df['Units_use'].join(self.df['Primary_Person_use'], on=["CRASH_ID"], how="inner")
                .filter(
                    ~self.df['Units_use'].VEH_BODY_STYL_ID.isin(
                        ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]
                    )
                )
                .filter(~self.df['Primary_Person_use'].PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"]))
                .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
                .count()
                .withColumn("row", row_number().over(window))
                .filter(col("row") == 1)
                .drop("row", "count")
            )
        except Exception as exception:
            print('Error::{}'.format(exception))

        #print(res_df.count())
        print("7. Output: \n")
        res_df.show()

    # Analysis 8
    def top5_zip_codes(self, output_path, format):
        """
        Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        Parameters:
            - output_path (str) : Output File path
            - format (str) : Output File format
        Returns:
            - (list) : Top 5 Zip Codes with highest number crashes
        """
        try:
            res_df = (
                self.df['Units_use'].join(self.df['Primary_Person_use'], on=["CRASH_ID"], how="inner")
                .dropna(subset=["DRVR_ZIP"])
                .filter(
                    col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
            )
            .groupby("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        except Exception as exception:
            
            print('Error::{}'.format(exception))

        return [row[0] for row in res_df.collect()]

    # Analysis 9
    def crash_ids_with_nodamage(self, output_path, format):
        """
        Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails InsuranceParameters:
            - output_path (str) : Output File path
            - format (str) : Output File format
        Returns:
            - (int) : Distinct rash Ids count
        """
        try:
            res_df = (self.df['Damages_use'].join(self.df['Units_use'], on=["CRASH_ID"], how="inner")
                .filter(((self.df['Units_use'].VEH_DMAG_SCL_1_ID > "DAMAGED 4")
                & (~self.df['Units_use'].VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
                ) | ((self.df['Units_use'].VEH_DMAG_SCL_2_ID > "DAMAGED 4")
                & (~self.df['Units_use'].VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
                )
            ).filter(self.df['Damages_use'].DAMAGED_PROPERTY == "NONE").filter(self.df['Units_use'].FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
            .select("CRASH_ID").distinct()
            )
        except Exception as exception:
            print('Error::{}'.format(exception))

        return res_df.count()
    
    # Analysis 10
    def top5_vehicle_makers_speeding_offence(self, output_path, format):
        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers,
        used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences     
        - output_path (str) : Output File path
            - format (str) : Output File format
        Returns:
            - (list) : Top 5 Vehicle makers
        """
        try:
            top_25_state_list = [row[0]
            for row in self.df['Units_use'].filter(
                col("VEH_LIC_STATE_ID").cast("int").isNull()
            ).groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()
            ]
            top_10_used_vehicle_colors = [row[0]
            for row in self.df['Units_use'].filter(self.df['Units_use'].VEH_COLOR_ID != "NA").groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()
            ]

            res_df = (self.df['Charges_use'].join(self.df['Primary_Person_use'], on=["CRASH_ID"], how="inner")
                .join(self.df['Units_use'], on=["CRASH_ID"], how="inner")
                .filter(self.df['Charges_use'].CHARGE.contains("SPEED"))
                .filter(self.df['Primary_Person_use'].DRVR_LIC_TYPE_ID.isin(
                    ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                )
            ).filter(self.df['Units_use'].VEH_COLOR_ID.isin(top_10_used_vehicle_colors)).filter(self.df['Units_use'].VEH_LIC_STATE_ID.isin(top_25_state_list))
            .groupby("VEH_MAKE_ID").count().orderBy(col("count").desc()).limit(5)
        )
        except Exception as exception:

            print('Error::{}'.format(exception))

        return [row[0] for row in res_df.collect()]
