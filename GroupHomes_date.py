import arcpy
import smtplib
import traceback
import datetime
import StringIO




arcpy.env.workspace = r"Database Connections\SDE@Planning_CLUSTER.sde"
arcpy.env.overwriteOutput = True
db_conn = r"Database Connections\SDE@Planning_CLUSTER.sde"


#   data
ComPlus_BusiLic = r"Database Connections\COMMPLUS.sde\COMPLUS.WPB_ALL_BUSINESSLICENSES"
Planning_Group_Homes = r"Planning.SDE.WPB_GIS_GROUP_HOMES"
Planning_Group_Homes_fullpath = r"Database Connections\SDE@Planning_CLUSTER.sde\Planning.SDE.WPB_GIS_GROUP_HOMES"
Fields = [Field.baseName.encode('ascii') for Field in arcpy.ListFields(Planning_Group_Homes) if Field.baseName not in
          ['OBJECTID', 'TYPE']]
query_layer = "GROUP_HOMES_QL"
group_homes_poly = "GROUP_HOMES_poly"
group_homes_points = "GROUP_HOMES_points"
group_homes = r"Database Connections\SDE@Planning_CLUSTER.sde\Planning.SDE.GroupHomes_complus"
spatialref = arcpy.Describe(r"Database Connections\SDE@Planning_CLUSTER.sde\Planning.SDE.LandUsePlanning")\
    .spatialReference.exportToString()
TempTable = r"Database Connections\SDE@Planning_CLUSTER.sde\Planning.SDE.TempTableGroupHomes"

#   This sql query defines the data set by selecting from All_BUSINESSLICENSES on Comprod
#   623110 Nursing Care Facilities
#   623210 Residential Mental Retardation Facilities
#   623220 Residential Mental Health and Substance Abuse Facilities
#   623311 Continuing Care Retirement Communities
#   623312 COU Homes for the Elderly
#   623990 COU Other Residential Care Facilities

Sql_copytable = "CATEGORY IN  ('623110','623210','623220','623311','623312','623990')\
                 AND STAT IN ('ACTIVE','PRINTED','HOLD')"


try:
    # create lists of license numbers from Community Plus and corresponding table in GIS Cluster
    ComplusLicenses = set()
    PlanningLicenses = set()

    with arcpy.da.SearchCursor(ComPlus_BusiLic, 'LICENSE', Sql_copytable) as ComplusUC:
        for record in ComplusUC:
            ComplusLicenses.add(record)

    with arcpy.da.SearchCursor(Planning_Group_Homes, 'LICENSE') as PlanGH:
        for record in PlanGH:
            PlanningLicenses.add(record)

    #   Use set calculations to discover set differences
    InComplus_NotInSDE = ComplusLicenses.difference(PlanningLicenses)
    InSDE_NotInComplus = PlanningLicenses.difference(ComplusLicenses)

    #  If InComplus_NotInSDE has members, append record to GIS_ALCOHOL_LICENSES, and add point to AlcoholLicense_complus
    #   If InSDE_NotInComplus has members, delete record from AlcoholLicense_complus and GIS_ALCOHOL_LICENSES
    if len(InComplus_NotInSDE) == 0:
        with open(r"C:\Users\jsawyer\Desktop\Tickets\18140 Group Homes\logfile.txt","a") as log:
            now = datetime.datetime.now().strftime("%Y-%d-%m")
            log.write("\n-----------------\n")
            log.write(now + " no new Group Homes found\n\n")

    else:
        arcpy.AcceptConnections(db_conn, True)
        arcpy.DisconnectUser(db_conn, 'ALL')
        arcpy.AcceptConnections(db_conn, False)

        #   InComplus_NotInSDE is in unicode, need it in plain ascii text for query

        InComplus_NotInSDE_tup = tuple([x[0].encode('ascii') for x in InComplus_NotInSDE])

        sqlquery = "LICENSE IN {}".format(InComplus_NotInSDE_tup)

        #   it doesnt like insert cursor, so make a temp table and append to that, then append to GIS_ALCOHOL_LICENSES

        arcpy.CreateTable_management(db_conn,"TempTableGroupHomes", Planning_Group_Homes)

        with arcpy.da.SearchCursor(ComPlus_BusiLic, Fields, sqlquery) as sc:
            with arcpy.da.InsertCursor(TempTable, Fields) as ic:
                for record in sc:
                    ic.insertRow(record)

        arcpy.Append_management(TempTable, Planning_Group_Homes)

        #   The following block creates a Query Layer from a join between the new licenses identified earlier and the
        #   parcels in which they reside, saves the Query layer as a polygon fc...
        #   changes that to point fc, then appends the points to GroupHomes_complus

        sql = "SELECT PARCELS.[OBJECTID],[OWNPARCELID] AS PARCELS_PCN,[SRCREF],[OWNTYPE]," \
              "[GISdata_GISADMIN_OwnerParcel_AR],[LASTUPDATE],[LASTEDITOR],[Shape],[PARCEL_ID] AS COMPLUS_PCN," \
              "[BUSINESS_ID],[LICENSE],[CATEGORY],[CATEGORY_DESC],[STAT],[ISSUE],[EXPIRATION],[BUS_NAME],[BUS_PROD]," \
              "[SERVICE],[ADRS1],[BUS_PHONE],[BUS_EMAIL],[TYPE] FROM [Planning].[sde].[PLANNINGPARCELS] PARCELS," \
              "[Planning].[sde].[WPB_GIS_GROUP_HOMES] GROUPHOMES WHERE PARCELS.OWNPARCELID = GROUPHOMES.PARCEL_ID " \
              "AND {}".format(sqlquery)

        arcpy.MakeQueryLayer_management(input_database=db_conn, out_layer_name=query_layer, query=sql,
                                        oid_fields= "OBJECTID", shape_type= "POLYGON", srid="2881",
                                        spatial_reference=spatialref)
        arcpy.management.CopyFeatures(query_layer, group_homes_poly, None, None, None, None)
        arcpy.FeatureToPoint_management(group_homes_poly, group_homes_points, "INSIDE")
        arcpy.Append_management(group_homes_points, group_homes)

        #   Create the alert email text. Uses StringIO to create a string treated as a file for formatting purposes

        TT_fieldnames =['PARCEL_ID', 'LICENSE', 'BUS_NAME', 'ADRS1']
        string_obj = StringIO.StringIO()
        with arcpy.da.SearchCursor(TempTable, TT_fieldnames) as TTSC:
            for row in TTSC:
                string_obj.write(''.join(row))
                string_obj.write('\n')

        report = string_obj.getvalue()

        today = datetime.datetime.now().strftime("%d-%m-%Y")
        subject = 'Group Homes report ' +  today
        sendto = ['cdglass@wpb.org','jssawyer@wpb.org'] # ,'JJudge@wpb.org','NKerr@wpb.org'
        sender = 'scriptmonitorwpb@gmail.com'
        sender_pw = "Bibby1997"
        server = 'smtp.gmail.com'
        body_text = "From: {0}\r\nTo: {1}\r\nSubject: {2}\r\nHere is a list of the new licenses.\n" \
                    "These have been added to GroupHomes_complus:\n\nPCN\t\tLicense Number\tBusiness Name\t" \
                    "Address\n\n{3}".format(sender, sendto, subject,report)

        gmail = smtplib.SMTP(server, 587)
        gmail.starttls()
        gmail.login(sender,sender_pw)
        gmail.sendmail(sender,sendto,body_text)
        gmail.quit()

        with open(r"C:\Users\jsawyer\Desktop\Tickets\18140 Group Homes\logfile.txt","a") as log:
            now = datetime.datetime.now().strftime("%Y-%d-%m")
            log.write("\n------------------------------------------\n\n")
            log.write(now)
            log.write('\n')
            log.write(report)
            log.write("\n")

    #   This section will delete from group_homes_complus and Planning.SDE.WPB_GIS_GROUP_HOMES any records that exists
    #   in Planning SDE but not in Complus (probably due to status change in complus)
    if len(InSDE_NotInComplus) == 0:
        with open(r"C:\Users\jsawyer\Desktop\Tickets\18140 Group Homes\logfile.txt", "a") as log:
            log.write("no new Group Homes deleted \n\n")

    else:
        InSDE_tup = tuple([record[0].encode('ascii').rstrip() for record in InSDE_NotInComplus])
        InSDE_query = "LICENSE IN {}".format(InSDE_tup)
        grouphomes_lyr = arcpy.MakeFeatureLayer_management(group_homes, 'grouphomeslyr')
        arcpy.SelectLayerByAttribute_management(grouphomes_lyr, "NEW_SELECTION", InSDE_query)
        #   ensures selection exists whose quantity equals number of licenses to remove
        #   so that DeleteFeatures doesnt delete entire fc
        if int(arcpy.GetCount_management(grouphomes_lyr)[0]) == (len(InSDE_NotInComplus) - 1):
            arcpy.DeleteFeatures_management(grouphomes_lyr)
        else:
        grouphomes_tblview = arcpy.MakeTableView_management(Planning_Group_Homes_fullpath, 'grouphomestblview')
        arcpy.SelectLayerByAttribute_management(grouphomes_tblview,"NEW_SELECTION", InSDE_query)
        #   ensures selection exists whose quantity equals number of licenses to remove
        #   so that DeleteFeatures doesnt delete entire fc
        if int(arcpy.GetCount_management(grouphomes_tblview)[0]) == (len(InSDE_NotInComplus) - 1):
            arcpy.DeleteRows_management(grouphomes_tblview)
        else:
            print "count of selected records in grouphomes_tbleview != len(InSDE_NotInComplus) line 173"

        today =  datetime.datetime.now().strftime("%d-%m-%Y")
        subject = 'Group Homes deleted licenses ' +  today
        sendto = ['cdglass@wpb.org','jssawyer@wpb.org'] # ,'JJudge@wpb.org','NKerr@wpb.org'
        sender = 'scriptmonitorwpb@gmail.com'
        sender_pw = "Bibby1997"
        server = 'smtp.gmail.com'
        body_text = "From: {0}\r\nTo: {1}\r\nSubject: {2}\r\nHere is a list license numbers of the deleted records.\nThese have been deleted from GroupHomes_complus feature class and Planning.SDE.WPB_GIS_GROUP_HOMES:\n{3}".format(sender, sendto, subject,InSDE_query)


        gmail = smtplib.SMTP(server, 587)
        gmail.starttls()
        gmail.login(sender,sender_pw)
        gmail.sendmail(sender,sendto,body_text)
        gmail.quit()

        with open(r"C:\Users\jsawyer\Desktop\Tickets\18140 Group Homes\logfile.txt","a") as log:
            now = datetime.datetime.now().strftime("%Y-%d-%m")
            log.write("\n------------------------------------------\n\n")
            log.write(now)
            log.write('\n')
            log.write('This license has been deleted:')
            log.write((", ").join(InSDE_NotInComplus))
            log.write("\n")

except Exception as E:
    arcpy.AcceptConnections(db_conn,True)

    today =  datetime.datetime.now().strftime("%Y-%d-%m")
    subject = 'Group Homes script failure report ' +  today
    sendto = "jssawyer@wpb.org" # ,'JJudge@wpb.org','NKerr@wpb.org'
    sender = 'scriptmonitorwpb@gmail.com'
    sender_pw = "Bibby1997"
    server = 'smtp.gmail.com'
    log = traceback.format_exc()
    body_text = "From: {0}\r\nTo: {1}\r\nSubject: {2}\r\nAn error occured. Here are the Type, arguements, " \
                "and log of the error\n\n{3}\n{4}\n{5}".format(sender, sendto, subject,type(E).__name__, E.args, log)

    gmail = smtplib.SMTP(server, 587)
    gmail.starttls()
    gmail.login(sender,sender_pw)
    gmail.sendmail(sender,sendto,body_text)
    gmail.quit()

    print body_text

finally:
    arcpy.AcceptConnections(db_conn, True)

    del_list = (TempTable, group_homes_poly, group_homes_points)
    for fc in del_list:
        if fc:
            arcpy.Delete_management(fc)
