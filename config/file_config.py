class ProductModel:
    
    @classmethod
    def get_config_feild_assist(cls):
        return {
            "Product_Name": {"rename": "product_name", "datatype": "str"},
            "ERP_ID": {"rename": "erp_id", "datatype": "str"},
            "Product_Division": {"rename": "product_division", "datatype": "str"},
            "PrimaryCategory": {"rename": "primary_category", "datatype": "str"},
            "SecondaryCategory": {"rename": "secondary_category", "datatype": "str"},
            "PTR": {"rename": "ptr", "datatype": "float"},
            "MRP": {"rename": "mrp", "datatype": "float"},
            "Unit": {"rename": "unit", "datatype": "str"},
            "Is_Active":{"rename":"product_status","datatype": "str"},
            # captured for incremental (delta) loading — stored in the blob parquet
            "Created_At": {"rename": "created_at", "datatype": "date"},
            "Updated_At": {"rename": "updated_at", "datatype": "date"},
        }
        
    @classmethod
    def get_config_ibeam(cls):
        return {
            "Item_Code": {"rename": "item_code", "datatype": "str"},
            "SKUs_Code": {"rename": "skus_code", "datatype": "str"},
            "Item_Name": {"rename": "item_name", "datatype": "str"},
            "Item_UOM": {"rename": "item_uom", "datatype": "str"},
            "Alternate_UOM": {"rename": "alternate_uom", "datatype": "str"},
            "Tax_Group_Code": {"rename": "tax_group_code", "datatype": "str"},
            "Tax_Group": {"rename": "tax_group", "datatype": "str"},
            "Item_Group_Code": {"rename": "item_group_code", "datatype": "str"},
            "Item_Group": {"rename": "item_group", "datatype": "str"},
            "LEVEL1": {"rename": "level1", "datatype": "str"},
            "LEVEL2": {"rename": "level2", "datatype": "str"},
            "LEVEL3": {"rename": "level3", "datatype": "str"},
            "LEVEL4": {"rename": "level4", "datatype": "str"},
            "LEVEL5": {"rename": "level5", "datatype": "str"}
        }    

class OutletModel:
    
    @classmethod
    def get_config_feild_assist(cls):
        return {
            "L1_Position_Name": {"rename": "l1_position_name", "datatype": "str"},
            "L1_Position_User_Name": {"rename": "l1_position_user_name", "datatype": "str"},
            "L1_Position_User_Erp_Id": {"rename": "l1_position_user_erp_id", "datatype": "str"},
            "L2_Position_Name": {"rename": "l2_position_name", "datatype": "str"},
            "L2_Position_User_Name": {"rename": "l2_position_user_name", "datatype": "str"},
            "L2_Position_User_Erp_Id": {"rename": "l2_position_user_erp_id", "datatype": "str"},
            "L3_Position_Name": {"rename": "l3_position_name", "datatype": "str"},
            "L3_Position_User_Name": {"rename": "l3_position_user_name", "datatype": "str"},
            "L3_Position_User_Erp_Id": {"rename": "l3_position_user_erp_id", "datatype": "str"},
            "L4_Position_Name": {"rename": "l4_position_name", "datatype": "str"},
            "L4_Position_User_Name": {"rename": "l4_position_user_name", "datatype": "str"},
            "L4_Position_User_Erp_Id": {"rename": "l4_position_user_erp_id", "datatype": "str"},
            "L5_Position_Name": {"rename": "l5_position_name", "datatype": "str"},
            "L5_Position_User_Name": {"rename": "l5_position_user_name", "datatype": "str"},
            "L5_Position_User_Erp_Id": {"rename": "l5_position_user_erp_id", "datatype": "str"},
            "Zone": {"rename": "zone", "datatype": "str"},
            "Region": {"rename": "region", "datatype": "str"},
            "Territory": {"rename": "territory", "datatype": "str"},
            "Beats": {"rename": "beats", "datatype": "str"},
            "BeatERPId": {"rename": "beaterp_id", "datatype": "str"},
            "SuperStockist_Erp_id": {"rename": "superstockist_erp_id", "datatype": "str"},
            "SuperStockist_Name": {"rename": "superstockist_name", "datatype": "str"},
            "Distributor": {"rename": "distributor", "datatype": "str"},
            "Distributor_Email_Id": {"rename": "distributor_email_id", "datatype": "str"},
            "Distributor_Erp_id": {"rename": "distributor_erp_id", "datatype": "str"},
            "Distributor_Place": {"rename": "distributor_place", "datatype": "str"},
            "Distributor_Type": {"rename": "distributor_type", "datatype": "str"},
            "Outlet_Erp_Id": {"rename": "outlet_erp_id", "datatype": "str"},
            "Outlets_Name": {"rename": "outlets_name", "datatype": "str"},
            "Latitude": {"rename": "latitude", "datatype": "float"},
            "Longitude": {"rename": "longitude", "datatype": "float"},
            "Owners_Name": {"rename": "owners_name", "datatype": "str"}, 
            "Owners_Number": {"rename": "owners_number", "datatype": "str"},
            "Outlets_Channel": {"rename": "outlets_channel", "datatype": "str"},
            "Outlets_Type": {"rename": "outlets_type", "datatype": "str"},
            "Formatted_Address": {"rename": "formatted_address", "datatype": "str"},
            "Address": {"rename": "address", "datatype": "str"},
            "SubCity": {"rename": "subcity", "datatype": "str"},
            "Market": {"rename": "market", "datatype": "str"},
            "City": {"rename": "city", "datatype": "str"},
            "State": {"rename": "state", "datatype": "str"},
            "Country": {"rename": "country", "datatype": "str"},
            "Pincode": {"rename": "pincode", "datatype": "str"},
            "GSTIN": {"rename": "gstin", "datatype": "str"},
            "IsBlocked": {"rename": "status", "datatype": "bool"},
            "Outlet_Creation_Date": {"rename": "outlet_creation_date", "datatype": "date"},
            "Outlet_deactivation_date": {"rename": "outlet_deactivation_date", "datatype": "date"},
            "Last_Modified_On": {"rename": "last_modified_on", "datatype": "date"},
            "Image": {"rename": "image", "datatype": "str"},
        }

class SecondaryDumModel:
    @classmethod
    def get_config_feild_assist(cls):
        return {
            "Order_No": {"rename": "order_no", "datatype": "int"},
            "L5Position": {"rename": "l5position", "datatype": "str"},
            "L4Position": {"rename": "l4position", "datatype": "str"},
            "L3Position": {"rename": "l3position", "datatype": "str"},
            "L2Position": {"rename": "l2position", "datatype": "str"},
            "User_Position_Name": {"rename": "user_position_name", "datatype": "str"},
            "L5Position_User": {"rename": "l5position_user", "datatype": "str"},
            "L4Position_User": {"rename": "l4position_user", "datatype": "str"},
            "L3Position_User": {"rename": "l3position_user", "datatype": "str"},
            "L2Position_User": {"rename": "l2position_user", "datatype": "str"},
            "User": {"rename": "user", "datatype": "str"},
            "ESM_Erp_Id": {"rename": "esm_erp_id", "datatype": "str"},
            "SuperStockist": {"rename": "superstockist", "datatype": "str"},
            "SuperStockist_ErpId": {"rename": "superstockist_erp_id", "datatype": "str"},
            "Distributor": {"rename": "distributor", "datatype": "str"},
            "Distributor_Erp_Id": {"rename": "distributor_erp_id", "datatype": "str"},
            "Distributor_GSTIN": {"rename": "distributor_gstin", "datatype": "str"},
            "Distributor_State": {"rename": "distributor_state", "datatype": "str"},
            # "Distributor_Place": {"rename": "distributor_place", "datatype": "str"},
            # "PAN_Number": {"rename": "pan_number", "datatype": "str"},
            "Zone": {"rename": "zone", "datatype": "str"},
            "Region": {"rename": "region", "datatype": "str"},
            "Territory": {"rename": "territory", "datatype": "str"},
            "Beat": {"rename": "beat", "datatype": "str"},
            "BeatERPId": {"rename": "beaterp_id", "datatype": "str"},
            "Outlet_Erp_Id": {"rename": "outlet_erp_id", "datatype": "str"},
            "Outlet_Name": {"rename": "outlet_name", "datatype": "str"},
            "Owners_Name": {"rename": "owners_name", "datatype": "str"},
            "Owners_No": {"rename": "owners_no", "datatype": "str"},
            "Outlet_GSTIN": {"rename": "outlet_gstin", "datatype": "str"},
            "Address": {"rename": "address", "datatype": "str"},
            "City": {"rename": "city", "datatype": "str"},
            "State": {"rename": "state", "datatype": "str"},
            "Market": {"rename": "market", "datatype": "str"},
            "Order_Date": {"rename": "order_date", "datatype": "date"},
            "ProductDivision": {"rename": "product_division", "datatype": "str"},
            "PrimaryCategory": {"rename": "primary_category", "datatype": "str"},
            "SecondaryCategory": {"rename": "secondary_category", "datatype": "str"},
            "Product": {"rename": "product_name", "datatype": "str"},
            "Product_ErpId": {"rename": "product_erp_id", "datatype": "str"},
            # "SKUs_Code": {"rename": "skus_code", "datatype": "str"},
            "Packaging_Type": {"rename": "packaging_type", "datatype": "str"},
            "Qty_SuperUnit": {"rename": "qty_superunit", "datatype": "float"},
            "SuperUnit":{"rename":"super_unit","datatype":"str"},
            "Qty_StdUnit": {"rename": "qty_stdunit", "datatype": "float"},
            "StdUnit":{"rename":"std_unit","datatype":"str"},
            "Standard_Unit_Conversion_Factor": {"rename": "standard_unit_conversion_factor", "datatype": "float"},
            "Gross_Qty": {"rename": "gross_qty", "datatype": "float"},
            "Qty_Unit": {"rename": "qty_unit", "datatype": "float"},
            "Unit":{"rename":"unit","datatype":"str"},
            "Product_MRP": {"rename": "product_mrp", "datatype": "float"},
            "Price": {"rename": "price", "datatype": "float"},
            "Actual_Price": {"rename": "actual_price", "datatype": "float"},
            "MRP": {"rename": "mrp", "datatype": "float"},
            "StdUnit_Price": {"rename": "stdunit_price", "datatype": "float"},
            "Gross_Value": {"rename": "gross_value", "datatype": "float"},
            "Sale_Value": {"rename": "sale_value", "datatype": "float"},
            "CGST": {"rename": "cgst", "datatype": "float"},
            "SGST": {"rename": "sgst", "datatype": "float"},
            "IGST": {"rename": "igst", "datatype": "float"},
            "NetValue": {"rename": "net_value", "datatype": "float"},
            "Net_Value_Tax_Inclusive": {"rename": "net_value_tax_inclusive", "datatype": "float"},
            "Visit_Latitude": {"rename": "visit_latitude", "datatype": "float"},
            "Visit_Longitude": {"rename": "visit_longitude", "datatype": "float"},
            "Deviation_From_Outlet":{"rename":"deviation_from_outlet","datatype":"float"},
            "Is_Telephonic": {"rename": "is_telephonic", "datatype": "bool"},
            "Call_Start_Time": {"rename": "call_start_time", "datatype": "datetime"},
            "Call_End_Time": {"rename": "call_end_time", "datatype": "datetime"},
            # "Time_Spent_on_Call_MM_SS":{"rename":"time_spent_on_call_sec","datatype":"str"},
            "No_Sales_Reason_Category": {"rename": "no_sales_reason_category", "datatype": "str"},
            "No_Sales_Reason": {"rename": "no_sales_reason", "datatype": "str"},
            "Outlet_Type": {"rename": "outlet_type", "datatype": "str"},
            "Outlet_Channel_Name": {"rename": "outlet_channel_name", "datatype": "str"},
            # "Pincode": {"rename": "pincode", "datatype": "str"},
        }      
              
class PrimarySalesModel:
    
    @classmethod
    def get_repose_config_tally(cls):
        return {
            # Document info
            "Date": {"rename": "date", "datatype": "date"},
            "DocID": {"rename": "doc_id", "datatype": "str"},
            "DocType": {"rename": "doc_type", "datatype": "str"},

            # Customer / party info
            "Country": {"rename": "country", "datatype": "str"},
            "PartyName": {"rename": "party_name", "datatype": "str"},
            "TargetGroup": {"rename": "target_group", "datatype": "str"},
            "GSTIN": {"rename": "gstin", "datatype": "str"},
            "CustomerState": {"rename": "customer_state", "datatype": "str"},
            "City": {"rename": "city", "datatype": "str"},
            "Salesman": {"rename": "salesman", "datatype": "str"},
            "DCPLevel": {"rename": "dcp_level", "datatype": "str"},
            "ZONE": {"rename": "zone", "datatype": "str"},

            # Item info
            "Item_Group": {"rename": "item_group", "datatype": "str"},
            "ItemName": {"rename": "item_name", "datatype": "str"},
            "Is_FOC": {"rename": "is_foc", "datatype": "bool"},
            "Size": {"rename": "size", "datatype": "str"},
            "Length": {"rename": "length", "datatype": "float"},
            "Breadth": {"rename": "breadth", "datatype": "float"},
            "Height": {"rename": "height", "datatype": "float"},
            "Is_Standard": {"rename": "is_standard", "datatype": "bool"},
            "Bed_Size_Type": {"rename": "bed_size_type", "datatype": "str"},
            "LEVEL1": {"rename": "level1", "datatype": "str"},
            "LEVEL2": {"rename": "level2", "datatype": "str"},
            "LEVEL3": {"rename": "level3", "datatype": "str"},
            "Brand_Label": {"rename": "brand_label", "datatype": "str"},
            "Item_HSN": {"rename": "item_hsn", "datatype": "str"},
            "MRP": {"rename": "mrp", "datatype": "float"},
            "Rate": {"rename": "rate", "datatype": "float"},


            # Financial / value info
            # "Batch_Number": {"rename": "batch_no", "datatype": "str"},
            "SaleQty": {"rename": "sale_qty", "datatype": "float"},
            "ItemUOM": {"rename": "item_uom", "datatype": "str"},
            "BASIC": {"rename": "basic", "datatype": "float"},
            "DISCOUNT": {"rename": "discount", "datatype": "float"},
            "SALE_VALUE": {"rename": "sale_value", "datatype": "float"},
            "TOTAL_TAX": {"rename": "total_tax", "datatype": "float"},
            "GROSS_VALUE": {"rename": "gross_value", "datatype": "float"},

            # Zone / RM cost info
            "SQ_FT": {"rename": "sq_ft", "datatype": "float"},
            "SQ_FT_RM_COST": {"rename": "sq_ft_rm_cost", "datatype": "float"},
            "TOTAL_RM_COST": {"rename": "total_rm_cost", "datatype": "float"},
            "Labour_Cost": {"rename": "labour_cost", "datatype": "float"},
            "Freight": {"rename": "freight", "datatype": "float"},
        }
        
    @classmethod
    def get_config_ibeam(cls):
        return  {
            "Country": {"rename": "country", "datatype": "str"},
            "BranhState": {"rename": "branch_state", "datatype": "str"},
            "Zone": {"rename": "zone", "datatype": "str"},
            "BranchName": {"rename": "branch_name", "datatype": "str"},
            "TargetGroup": {"rename": "target_group", "datatype": "str"},
            "DCPLevel": {"rename": "dcp_level", "datatype": "str"},
            "PartyCode": {"rename": "party_code", "datatype": "str"},
            "PartyName": {"rename": "party_name", "datatype": "str"},
            "PartyBranch": {"rename": "party_branch", "datatype": "str"},
            "PartyBranchName": {"rename": "party_branch_name", "datatype": "str"},
            "TIN": {"rename": "tin", "datatype": "str"},
            "PartyAddress": {"rename": "party_address", "datatype": "str"},
            "CustomerState": {"rename": "customer_state", "datatype": "str"},
            "CustomerCountry": {"rename": "customer_country", "datatype": "str"},
            "BranchCity": {"rename": "branch_city", "datatype": "str"},
            "Party_Pincode": {"rename": "party_pincode", "datatype": "str"},#new fields added
            "Party_Contact_Person": {"rename": "party_contact_person", "datatype": "str"},#new fields added
            "Customer_Contact_No": {"rename": "customer_contact_no", "datatype": "str"},#new fields added
            "Customer_Alt_Contact_No": {"rename": "customer_alt_contact_no", "datatype": "str"},#new fields added
            "Party_E_Mail": {"rename": "party_email", "datatype": "str"},#new fields added
            "CustomerDistrict": {"rename": "customer_district", "datatype": "str"},#new fields added
            "Date": {"rename": "date", "datatype": "date"},
            "DocID": {"rename": "doc_id", "datatype": "str"},
            "DocType": {"rename": "doc_type", "datatype": "str"},
            "ProfitCenter": {"rename": "profit_center", "datatype": "str"},
            "Brand": {"rename": "brand", "datatype": "str"},
            "Item_Group": {"rename": "item_group", "datatype": "str"},
            "LEVEL1": {"rename": "level1", "datatype": "str"},
            "LEVEL2": {"rename": "level2", "datatype": "str"},
            "LEVEL3": {"rename": "level3", "datatype": "str"},
            "LEVEL4": {"rename": "level4", "datatype": "str"},
            "LEVEL5": {"rename": "level5", "datatype": "str"},
            "ItemCode": {"rename": "item_code", "datatype": "str"},
            "SKUs_Code": {"rename": "skus_code", "datatype": "str"},
            "ItemName": {"rename": "item_name", "datatype": "str"},
            "ItemUOM": {"rename": "item_uom", "datatype": "str"},
            "SaleQty": {"rename": "sale_qty", "datatype": "float"},
            "BASIC": {"rename": "basic", "datatype": "float"},
            "DISCOUNT_PERCENT": {"rename": "discount", "datatype": "float"},
            "TAXABLE_VAL": {"rename": "taxable_val", "datatype": "float"},
            "GROSS_VALUE": {"rename": "gross_value", "datatype": "float"},
            "IGST_5": {"rename": "igst_5", "datatype": "float"},
            "IGST_12": {"rename": "igst_12", "datatype": "float"},
            "IGST_18": {"rename": "igst_18", "datatype": "float"},
            "IGST": {"rename": "igst", "datatype": "float"},
            "CGST": {"rename": "cgst", "datatype": "float"},
            "CGST_2_5": {"rename": "cgst_2_5", "datatype": "float"},
            "SGST": {"rename": "sgst", "datatype": "float"},
            "SGST_2_5": {"rename": "sgst_2_5", "datatype": "float"},
            "CGST_6": {"rename": "cgst_6", "datatype": "float"},
            "SGST_6": {"rename": "sgst_6", "datatype": "float"},
            "CGST_9": {"rename": "cgst_9", "datatype": "float"},
            "SGST_9": {"rename": "sgst_9", "datatype": "float"},
            "SALE_VALUE": {"rename": "sale_value", "datatype": "float"},
            "RQTY": {"rename": "rqty", "datatype": "float"},
            "RETURN_VALUE": {"rename": "return_value", "datatype": "float"},
            "total_tax": {"rename": "total_tax", "datatype": "float"},
            "Rate": {"rename": "rate", "datatype": "float"}   #new fields added
        }       

class ProductionRegistrationModel:
    @classmethod
    def get_config_ibeam(cls):
        return {
            "Branch_Name": {"rename": "branch_name", "datatype": "str"},
            "Date": {"rename": "doc_date", "datatype": "date"},
            "Location": {"rename": "location", "datatype": "str"},
            "Batch_Code": {"rename": "batch_code", "datatype": "str"},
            "LOT_Number": {"rename": "lot_number", "datatype": "str"},
            "Manufacture_Date": {"rename": "manufacture_date", "datatype": "date"},
            "Expiry_Date": {"rename": "expiry_date", "datatype": "date"},
            "Production_Id": {"rename": "production_id", "datatype": "str"},
            "Item_Code": {"rename": "item_code", "datatype": "str"},
            "Item_Code2": {"rename": "item_code2", "datatype": "str"},
            "Item_Name": {"rename": "item_name", "datatype": "str"},
            "Item_UOM": {"rename": "item_uom", "datatype": "str"},
            "Production_Qty": {"rename": "production_qty", "datatype": "float"},
            "Production_Value": {"rename": "production_value", "datatype": "float"},
            "Material_Cost": {"rename": "material_cost", "datatype": "float"},
            "Mfg_Cost": {"rename": "mfg_cost", "datatype": "float"},
            "Standard_Production_Value": {"rename": "standard_production_value", "datatype": "float"},
            "Standard_PROD_Qty": {"rename": "standard_prod_qty", "datatype": "float"},
            "Standard_Material_Qty": {"rename": "standard_material_qty", "datatype": "float"},
            "Standard_Material_Cost": {"rename": "standard_material_cost", "datatype": "float"},
            "Standard_Mfg_Cost": {"rename": "standard_mfg_cost", "datatype": "float"},
            "BOM_Name": {"rename": "bom_name", "datatype": "str"}
        }

class DebtorsMasterModel:
    @classmethod
    def get_config_ibeam(cls):
        return {
            "Party_Code": {"rename": "party_code", "datatype": "str"},
            "Party_Name": {"rename": "party_name", "datatype": "str"},
            "Party_Branch_Code": {"rename": "party_branch_code", "datatype": "str"},
            "Party_Branch_Name": {"rename": "party_branch_name", "datatype": "str"},
            "Address": {"rename": "address_text", "datatype": "str"},
            "City_Code": {"rename": "city_code", "datatype": "str"},
            "City": {"rename": "city_name", "datatype": "str"},
            "State_Code": {"rename": "state_code", "datatype": "str"},
            "State": {"rename": "state_name", "datatype": "str"},
            "Country_Code": {"rename": "country_code", "datatype": "str"},
            "Country_Name": {"rename": "country_name", "datatype": "str"},
            "DCP_Level": {"rename": "dcp_level", "datatype": "str"},
            "Trade_Group": {"rename": "trade_group", "datatype": "str"},
            "Ledger_Code": {"rename": "ledger_code", "datatype": "str"},
            "Ledger_Name": {"rename": "ledger_name", "datatype": "str"},
            "Subledger_Type": {"rename": "subledger_type", "datatype": "str"}
            }

class CreditorsModel:
    @classmethod
    def get_config_ibeam(cls):
        return {
            "Party_Code": {"rename": "party_code", "datatype": "str"},
            "Party_Name": {"rename": "party_name", "datatype": "str"},
            "Party_Address": {"rename": "party_address", "datatype": "str"},
            "Subledger_Type": {"rename": "subledger_type", "datatype": "str"},
            "Tax_Subnature": {"rename": "tax_subnature", "datatype": "str"},
            "Tax_Nature": {"rename": "tax_nature", "datatype": "str"},
            "City_Code": {"rename": "city_code", "datatype": "str"},
            "City": {"rename": "city_name", "datatype": "str"},
            "State_Code": {"rename": "state_code", "datatype": "str"},
            "State": {"rename": "state_name", "datatype": "str"},
            "Country_Code": {"rename": "country_code", "datatype": "str"},
            "Country": {"rename": "country_name", "datatype": "str"},
            "Ledger_Code": {"rename": "ledger_code", "datatype": "str"},
            "Ledger_Name": {"rename": "ledger_name", "datatype": "str"}
        }

class InventoryDataModel:
    @classmethod
    def get_config_ibeam(cls):
        return {
            "SystemID": {"rename": "ext_erp_record_id", "datatype": "str"},
            "Branch": {"rename": "branch_name", "datatype": "str"},
            "Location": {"rename": "warehouse_name", "datatype": "str"},
            "Date": {"rename": "transaction_date", "datatype": "date"},
            "Document_Name": {"rename": "document_name", "datatype": "str"},
            "DOC_NO": {"rename": "document_number", "datatype": "str"},
            "Original_Reciept_ID": {"rename": "original_receipt_id", "datatype": "str"},
            "Item_Code": {"rename": "product_code", "datatype": "str"},
            "Item_Code2": {"rename": "product_code2", "datatype": "str"},
            "Item": {"rename": "product_name", "datatype": "str"},
            "Item_UOM": {"rename": "item_uom_code", "datatype": "str"},
            "Root_Level": {"rename": "root_level", "datatype": "str"},
            "LEVEL1": {"rename": "level1", "datatype": "str"},
            "LEVEL2": {"rename": "level2", "datatype": "str"},
            "LEVEL3": {"rename": "level3", "datatype": "str"},
            "LEVEL4": {"rename": "level4", "datatype": "str"},
            "LEVEL5": {"rename": "level5", "datatype": "str"},
            "Inward_Quantity": {"rename": "inward_qty", "datatype": "float"},
            "Inward_Value": {"rename": "inward_amount", "datatype": "float"},
            "Outward_Quantity": {"rename": "outward_qty", "datatype": "float"},
            "Outward_Value": {"rename": "outward_amount", "datatype": "float"},
            "Remarks": {"rename": "remarks", "datatype": "str"}
        }

class ItemMasterModel:
    @classmethod
    def get_config_ibeam(cls):
        return {
            "Item_Code": {"rename": "item_code", "datatype": "str"},
            "SKUs_Code": {"rename": "skus_code", "datatype": "str"},
            "Item_Name": {"rename": "item_name", "datatype": "str"},
            "Item_UOM": {"rename": "item_uom", "datatype": "str"},
            "Alternate_UOM": {"rename": "alternate_uom", "datatype": "str"},
            "Tax_Group_Code": {"rename": "tax_group_code", "datatype": "str"},
            "Tax_Group": {"rename": "tax_group", "datatype": "str"},
            "Item_Group_Code": {"rename": "item_group_code", "datatype": "str"},
            "Item_Group": {"rename": "item_group", "datatype": "str"},
            "LEVEL1": {"rename": "level1", "datatype": "str"},
            "LEVEL2": {"rename": "level2", "datatype": "str"},
            "LEVEL3": {"rename": "level3", "datatype": "str"},
            "LEVEL4": {"rename": "level4", "datatype": "str"},
            "LEVEL5": {"rename": "level5", "datatype": "str"}
        }
    
    @classmethod
    def get_repose_config_tally(cls):
        return {
            "Item_Name": {"rename": "item_name", "datatype": "str"},
            "Size": {"rename": "size", "datatype": "str"},
            "Length": {"rename": "length", "datatype": "float"},
            "Breadth": {"rename": "breadth", "datatype": "float"},
            "Height": {"rename": "height", "datatype": "float"},
            "MRP": {"rename": "mrp", "datatype": "float"},
            "Item_UOM": {"rename": "item_uom", "datatype": "str"},
            "Is_Standard": {"rename": "is_standard", "datatype": "bool"},
            "Bed_Size_Type": {"rename": "bed_size_type", "datatype": "str"},
            "Item_Code": {"rename": "item_code", "datatype": "str"},
            "SKUs_Code": {"rename": "skus_code", "datatype": "str"},
            "Alternate_UOM": {"rename": "alternate_uom", "datatype": "str"},
            "Tax_Group_Code": {"rename": "tax_group_code", "datatype": "str"},
            "Tax_Group": {"rename": "tax_group", "datatype": "str"},
            "Item_Group_Code": {"rename": "item_group_code", "datatype": "str"},
            "Item_Group": {"rename": "item_group", "datatype": "str"},
            "LEVEL1": {"rename": "level1", "datatype": "str"},
            "LEVEL2": {"rename": "level2", "datatype": "str"},
            "LEVEL3": {"rename": "level3", "datatype": "str"},
            "LEVEL4": {"rename": "level4", "datatype": "str"},
            "LEVEL5": {"rename": "level5", "datatype": "str"}
        }

class FinanceDataModel:
    @classmethod
    def get_config_ibeam(cls):
        return {
            "ControlCode": {"rename": "ext_erp_record_id", "datatype": "str"},
            "Branch_Code": {"rename": "branch_code", "datatype": "str"},
            "Branch": {"rename": "branch_name", "datatype": "str"},
            "Ledger_Name": {"rename": "ledger_name", "datatype": "str"},
            "Ledger_Code": {"rename": "ledger_code", "datatype": "str"},
            "Root_Level": {"rename": "group_class", "datatype": "str"},
            "LEVEL1": {"rename": "level1", "datatype": "str"},
            "LEVEL2": {"rename": "level2", "datatype": "str"},
            "LEVEL3": {"rename": "level3", "datatype": "str"},
            "LEVEL4": {"rename": "level4", "datatype": "str"},
            "LEVEL5": {"rename": "level5", "datatype": "str"},
            "VCH_AMT": {"rename": "amount", "datatype": "float"},
            "DRCR": {"rename": "drcr", "datatype": "str"},
            "TRANSACTION_BRANCH": {"rename": "branch_code", "datatype": "str"},
            "VCHTYPNM": {"rename": "vch_type_name", "datatype": "str"},
            "VCH_PREFIX": {"rename": "vch_prefix", "datatype": "str"},
            "VCHCODE": {"rename": "vch_code", "datatype": "str"},
            "VCH_NUM": {"rename": "vch_number", "datatype": "str"},
            "VCH_DATE": {"rename": "transaction_date", "datatype": "date"},
            "VALUE_DATE": {"rename": "value_date", "datatype": "date"},
            "CHEQ_NO": {"rename": "cheq_no", "datatype": "str"},
            "CHEQ_DATE": {"rename": "cheq_date", "datatype": "date"},
            "SUBLEDNM": {"rename": "subledger_name", "datatype": "str"},
            "SUBLEDCD": {"rename": "subledger_code", "datatype": "str"},
            "REF_DOC_TYPE": {"rename": "ref_doc_type", "datatype": "str"},
            "REF_DOC_NUM": {"rename": "ref_doc_num", "datatype": "str"},
            "REF_DOC_DATE": {"rename": "ref_doc_date", "datatype": "date"},
            "NARRATION": {"rename": "narration", "datatype": "str"},
            "COST_LEVEL1": {"rename": "category_id", "datatype": "str"},
            "COST_LEVEL2": {"rename": "cost_level2", "datatype": "str"}
        }

class ReceivablesModel:
    @classmethod
    def get_config_ibeam(cls):
        return {
            "BRANCH_CODE": {"rename": "branch_code", "datatype": "str"},
            "BRANCH": {"rename": "branch_name", "datatype": "str"},
            "PARTY_TYPE": {"rename": "party_type", "datatype": "str"},
            "LEDGER": {"rename": "ledger_name", "datatype": "str"},
            "SUB_LEDGER_CODE": {"rename": "subledger_code", "datatype": "str"},
            "SUB_LEDGER": {"rename": "subledger_name", "datatype": "str"},
            "DOC_TYPE": {"rename": "document_type", "datatype": "str"},
            "DOC_NO": {"rename": "document_number", "datatype": "str"},
            "DOC_DATE": {"rename": "document_date", "datatype": "date"},
            "BILL_NO": {"rename": "bill_number", "datatype": "str"},
            "BILL_DATE": {"rename": "bill_date", "datatype": "date"},
            "ADJ_DOC_TYPE": {"rename": "adjustment_document_type", "datatype": "str"},
            "DUE_DATE": {"rename": "due_date", "datatype": "date"},
            "GROSS_AMT": {"rename": "gross_amount", "datatype": "float"},
            "NET_AMT": {"rename": "net_amount", "datatype": "float"},
            "ADJ_AMT": {"rename": "adjusted_amount", "datatype": "float"},
            "BALANCE": {"rename": "balance_amount", "datatype": "float"},
            "OVER_DUE": {"rename": "overdue_amount", "datatype": "float"},
            "DR_CR": {"rename": "debit_credit_indicator", "datatype": "str"},
            "Ref_Doc": {"rename": "reference_document", "datatype": "str"},
            "Ref_Doc_No": {"rename": "reference_document_number", "datatype": "str"},
            "REF_DOC_DATE": {"rename": "reference_document_date", "datatype": "date"},
            "REMARKS": {"rename": "remarks", "datatype": "str"},
            "VOUCHER_NARRATION": {"rename": "voucher_narration", "datatype": "str"},
            "TRANSACTION_NARRATION": {"rename": "transaction_narration", "datatype": "str"},
            "BILL_NARRATION": {"rename": "bill_narration", "datatype": "str"}
        }

class PayablesModel:
    @classmethod
    def get_config_ibeam(cls):
        return {
            "BRANCH_CODE": {"rename": "branch_code", "datatype": "str"},
            "BRANCH": {"rename": "branch_name", "datatype": "str"},
            "PARTY_TYPE": {"rename": "party_type", "datatype": "str"},
            "LEDGER": {"rename": "ledger_name", "datatype": "str"},
            "SUB_LEDGER_CODE": {"rename": "subledger_code", "datatype": "str"},
            "SUB_LEDGER": {"rename": "subledger_name", "datatype": "str"},
            "DOC_TYPE": {"rename": "document_type", "datatype": "str"},
            "DOC_NO": {"rename": "document_number", "datatype": "str"},
            "DOC_DATE": {"rename": "document_date", "datatype": "date"},
            "BILL_NO": {"rename": "bill_number", "datatype": "str"},
            "BILL_DATE": {"rename": "bill_date", "datatype": "date"},
            "ADJ_DOC_TYPE": {"rename": "adjustment_document_type", "datatype": "str"},
            "DUE_DATE": {"rename": "due_date", "datatype": "date"},
            "GROSS_AMT": {"rename": "gross_amount", "datatype": "float"},
            "NET_AMT": {"rename": "net_amount", "datatype": "float"},
            "ADJ_AMT": {"rename": "adjusted_amount", "datatype": "float"},
            "BALANCE": {"rename": "balance_amount", "datatype": "float"},
            "OVER_DUE": {"rename": "overdue_amount", "datatype": "float"},
            "DR_CR": {"rename": "debit_credit_indicator", "datatype": "str"},
            "Ref_Doc": {"rename": "reference_document", "datatype": "str"},
            "Ref_Doc_No": {"rename": "reference_document_number", "datatype": "str"},
            "REF_DOC_DATE": {"rename": "reference_document_date", "datatype": "date"},
            "REMARKS": {"rename": "remarks", "datatype": "str"},
            "VOUCHER_NARRATION": {"rename": "voucher_narration", "datatype": "str"},
            "TRANSACTION_NARRATION": {"rename": "transaction_narration", "datatype": "str"},
            "BILL_NARRATION": {"rename": "bill_narration", "datatype": "str"}
        }


class QcomSalesModel:
    """Q-commerce sales source tables (pbisalesanalysisdb). One config per
    platform; every platform renames to the SAME canonical names so the
    serializer, flow config and loader use one naming throughout:

        sales_id, order_date, item_code, product_name, category (leaf),
        city_name, gross_qty, gross_value, product_mrp, created_date, modified_date
        (+ platform-specific: l1_category/l2_category, brand_name, area_name,
         store_id, is_combo, combo_item_code, combo_units_sold,
         manufacturer_name, manufacturer_erp_id)
    """

    @classmethod
    def get_config_blinkit(cls):
        # Hangyo_Blinkit_Overall_Sales. NOTE: source `mrp` is the GROSS value
        # (line total), not a unit MRP -> renamed to gross_value.
        return {
            "id": {"rename": "sales_id", "datatype": "str"},
            "date": {"rename": "order_date", "datatype": "date"},
            "item_id": {"rename": "item_code", "datatype": "str"},
            "item_name": {"rename": "product_name", "datatype": "str"},
            "category": {"rename": "category", "datatype": "str"},
            "manufacturer_id": {"rename": "manufacturer_erp_id", "datatype": "str"},
            "manufacturer_name": {"rename": "manufacturer_name", "datatype": "str"},
            "city_consolidated": {"rename": "city_name", "datatype": "str"},
            "qty_sold": {"rename": "gross_qty", "datatype": "float"},
            "mrp": {"rename": "gross_value", "datatype": "float"},
            "created_date": {"rename": "created_date", "datatype": "date"},
            "modified_date": {"rename": "modified_date", "datatype": "date"},
        }

    @classmethod
    def get_config_instamart(cls):
        # Hangyo_Instamart_Overall_Sales. 3-level category: L1 > L2 > L3; L3 is
        # the leaf the product/sale point at -> canonical `category`.
        # BASE_MRP is the unit MRP, GMV the gross value. VARIANT not taken.
        return {
            "id": {"rename": "sales_id", "datatype": "str"},
            "Ordered_Date": {"rename": "order_date", "datatype": "date"},
            "ITEM_CODE": {"rename": "item_code", "datatype": "str"},
            "PRODUCT_NAME": {"rename": "product_name", "datatype": "str"},
            "L1_CATEGORY": {"rename": "l1_category", "datatype": "str"},
            "L2_CATEGORY": {"rename": "l2_category", "datatype": "str"},
            "L3_CATEGORY": {"rename": "category", "datatype": "str"},
            "BRAND": {"rename": "brand_name", "datatype": "str"},
            "city_consolidated": {"rename": "city_name", "datatype": "str"},
            "AREA_NAME": {"rename": "area_name", "datatype": "str"},
            "STORE_ID": {"rename": "store_id", "datatype": "str"},
            "COMBO": {"rename": "is_combo", "datatype": "str"},
            "COMBO_ITEM_CODE": {"rename": "combo_item_code", "datatype": "str"},
            "COMBO_UNITS_SOLD": {"rename": "combo_units_sold", "datatype": "float"},
            "BASE_MRP": {"rename": "product_mrp", "datatype": "float"},
            "UNITS_SOLD": {"rename": "gross_qty", "datatype": "float"},
            "GMV": {"rename": "gross_value", "datatype": "float"},
            "created_date": {"rename": "created_date", "datatype": "date"},
            "modified_date": {"rename": "modified_date", "datatype": "date"},
        }

    @classmethod
    def get_config_zepto(cls):
        # hangyo_Zepto_sales. SKUNumber (UUID) is the product key; 2-level
        # category SKUCategory > SKUSubCategory — the sub-category is the leaf
        # the product/sale point at -> canonical `category`. MRP is the unit MRP,
        # GrossMerchandiseValue the gross value. EAN -> product.bar_code.
        return {
            "id": {"rename": "sales_id", "datatype": "str"},
            "Date": {"rename": "order_date", "datatype": "date"},
            "SKUNumber": {"rename": "item_code", "datatype": "str"},
            "SKUName": {"rename": "product_name", "datatype": "str"},
            "EAN": {"rename": "ean", "datatype": "str"},
            "SKUCategory": {"rename": "l1_category", "datatype": "str"},
            "SKUSubCategory": {"rename": "category", "datatype": "str"},
            "BrandName": {"rename": "brand_name", "datatype": "str"},
            "ManufacturerName": {"rename": "manufacturer_name", "datatype": "str"},
            "ManufacturerID": {"rename": "manufacturer_erp_id", "datatype": "str"},
            "city_consolidated": {"rename": "city_name", "datatype": "str"},
            "SalesQtyUnits": {"rename": "gross_qty", "datatype": "float"},
            "MRP": {"rename": "product_mrp", "datatype": "float"},
            "GrossMerchandiseValue": {"rename": "gross_value", "datatype": "float"},
            "created_date": {"rename": "created_date", "datatype": "date"},
            "modified_date": {"rename": "modified_date", "datatype": "date"},
        }


class QcomStockModel:
    """Q-commerce stock-on-hand source tables (pbisalesanalysisdb). One config
    per platform; all rename to the SAME canonical names:

        stock_id, snapshot_date, item_code, product_name, city_name, qty_on_hand,
        created_date, modified_date
        (+ platform-specific: l1_category, category, ean, facility_erp_id, facility_name,
         storage_type, qty_backend, qty_frontend, days_on_hand, shelf_life_days,
         potential_gmv_loss, open_po_qty, open_po_refs, brand_name,
         manufacturer_name, manufacturer_erp_id)
    """

    @classmethod
    def get_config_blinkit(cls):
        # hangyo_blinkit_stock_on_hand. No category column: category comes from
        # the product (flow). City = city_consolidated (ETL team's standard name).
        return {
            "id": {"rename": "stock_id", "datatype": "str"},
            "snapshot_date": {"rename": "snapshot_date", "datatype": "date"},
            "item_id": {"rename": "item_code", "datatype": "str"},
            "item_name": {"rename": "product_name", "datatype": "str"},
            "backend_facility_id": {"rename": "facility_erp_id", "datatype": "str"},
            "backend_facility_name": {"rename": "facility_name", "datatype": "str"},
            "city_consolidated": {"rename": "city_name", "datatype": "str"},
            "backend_inv_qty": {"rename": "qty_backend", "datatype": "float"},
            "frontend_inv_qty": {"rename": "qty_frontend", "datatype": "float"},
            "created_date": {"rename": "created_date", "datatype": "date"},
            "modified_date": {"rename": "modified_date", "datatype": "date"},
        }

    @classmethod
    def get_config_instamart(cls):
        # hangyo_instamart_stock_on_hand. 2-level category L1 > L2 (leaf = `category`).
        return {
            "id": {"rename": "stock_id", "datatype": "str"},
            "snapshot_date": {"rename": "snapshot_date", "datatype": "date"},
            "SkuCode": {"rename": "item_code", "datatype": "str"},
            "SkuDescription": {"rename": "product_name", "datatype": "str"},
            "L1": {"rename": "l1_category", "datatype": "str"},
            "L2": {"rename": "category", "datatype": "str"},
            "city_consolidated": {"rename": "city_name", "datatype": "str"},
            "FacilityName": {"rename": "facility_name", "datatype": "str"},
            "StorageType": {"rename": "storage_type", "datatype": "str"},
            "WarehouseQtyAvailable": {"rename": "qty_on_hand", "datatype": "float"},
            "DaysOnHand": {"rename": "days_on_hand", "datatype": "int"},
            "ShelfLifeDays": {"rename": "shelf_life_days", "datatype": "int"},
            "PotentialGmvLoss": {"rename": "potential_gmv_loss", "datatype": "float"},
            "OpenPoQuantity": {"rename": "open_po_qty", "datatype": "float"},
            "OpenPos": {"rename": "open_po_refs", "datatype": "str"},
            "created_date": {"rename": "created_date", "datatype": "date"},
            "modified_date": {"rename": "modified_date", "datatype": "date"},
        }

    @classmethod
    def get_config_zepto(cls):
        # hangyo_zepto_vendor_inventory. 2-level category SKU_Category > SKU_Sub_Category
        # (leaf = `category`), EAN -> product.bar_code — same as Zepto sales.
        return {
            "id": {"rename": "stock_id", "datatype": "str"},
            "snapshot_date": {"rename": "snapshot_date", "datatype": "date"},
            "SKU_Code": {"rename": "item_code", "datatype": "str"},
            "SKU_Name": {"rename": "product_name", "datatype": "str"},
            "EAN": {"rename": "ean", "datatype": "str"},
            "SKU_Category": {"rename": "l1_category", "datatype": "str"},
            "SKU_Sub_Category": {"rename": "category", "datatype": "str"},
            "city_consolidated": {"rename": "city_name", "datatype": "str"},
            "Brand_Name": {"rename": "brand_name", "datatype": "str"},
            "Manufacturer_Name": {"rename": "manufacturer_name", "datatype": "str"},
            "Manufacturer_ID": {"rename": "manufacturer_erp_id", "datatype": "str"},
            "Units": {"rename": "qty_on_hand", "datatype": "float"},
            "created_date": {"rename": "created_date", "datatype": "date"},
            "modified_date": {"rename": "modified_date", "datatype": "date"},
        }


class QcomPoModel:
    """Q-commerce purchase-order source tables (pbisalesanalysisdb). One config
    per platform; all rename to the SAME canonical names:

        po_id, snapshot_date, po_number, po_date, po_expiry_date, po_status_raw,
        vendor_name, item_code, product_name, facility_name, ordered_qty,
        received_qty, pending_qty, unit_base_cost, product_mrp,
        line_value_with_tax, created_date, modified_date
        (+ platform-specific: category_path, entity_name, vendor_code, taxes, ...)

    po_status / received_qty (Blinkit) / pending_qty (Zepto) are derived by
    the serializer (engine.qcom_serializers).
    """

    @classmethod
    def get_config_blinkit(cls):
        # hangyo_blinkit_po. No category column: category comes from the product
        # (flow). City = city_consolidated (ETL team's standard name).
        return {
            "id": {"rename": "po_id", "datatype": "str"},
            "snapshot_date": {"rename": "snapshot_date", "datatype": "date"},
            "po_number": {"rename": "po_number", "datatype": "str"},
            "order_date": {"rename": "po_date", "datatype": "date"},
            "expiry_date": {"rename": "po_expiry_date", "datatype": "date"},
            "po_state": {"rename": "po_status_raw", "datatype": "str"},
            "vendor_name": {"rename": "vendor_name", "datatype": "str"},
            "entity_vendor_legal_name": {"rename": "entity_name", "datatype": "str"},
            "manufacturer_name": {"rename": "manufacturer_name", "datatype": "str"},
            "facility_name": {"rename": "facility_name", "datatype": "str"},
            "city_consolidated": {"rename": "city_name", "datatype": "str"},
            "item_id": {"rename": "item_code", "datatype": "str"},
            "name": {"rename": "product_name", "datatype": "str"},
            "upc": {"rename": "product_ean", "datatype": "str"},
            "uom_text": {"rename": "pack_size_text", "datatype": "str"},
            "units_ordered": {"rename": "ordered_qty", "datatype": "float"},
            "remaining_quantity": {"rename": "pending_qty", "datatype": "float"},
            "cost_price": {"rename": "unit_base_cost", "datatype": "float"},
            "landing_rate": {"rename": "unit_landing_cost", "datatype": "float"},
            "mrp": {"rename": "product_mrp", "datatype": "float"},
            "margin_percentage": {"rename": "margin_percentage", "datatype": "float"},
            "total_amount": {"rename": "line_value_with_tax", "datatype": "float"},
            "tax_value": {"rename": "tax_pct", "datatype": "float"},
            "cgst_value": {"rename": "cgst_pct", "datatype": "float"},
            "sgst_value": {"rename": "sgst_pct", "datatype": "float"},
            "igst_value": {"rename": "igst_pct", "datatype": "float"},
            "cess_value": {"rename": "cess_pct", "datatype": "float"},
            "created_date": {"rename": "created_date", "datatype": "date"},
            "modified_date": {"rename": "modified_date", "datatype": "date"},
        }

    @classmethod
    def get_config_instamart(cls):
        # hangyo_instamart_po. CategoryId is the path "L1, L2" (split by the
        # serializer into l1_category > category).
        return {
            "id": {"rename": "po_id", "datatype": "str"},
            "snapshot_date": {"rename": "snapshot_date", "datatype": "date"},
            "PoNumber": {"rename": "po_number", "datatype": "str"},
            "PoCreatedAt": {"rename": "po_date", "datatype": "date"},
            "PoModifiedAt": {"rename": "po_modified_at", "datatype": "date"},
            "PoExpiryDate": {"rename": "po_expiry_date", "datatype": "date"},
            "ExpectedDeliveryDate": {"rename": "expected_delivery_date", "datatype": "date"},
            "Status": {"rename": "po_status_raw", "datatype": "str"},
            "PoAgeing": {"rename": "po_ageing_days", "datatype": "int"},
            "InternalExternalPo": {"rename": "internal_external", "datatype": "str"},
            "OtbReferenceNumber": {"rename": "otb_reference_number", "datatype": "str"},
            "ReferencePoNumber": {"rename": "reference_po_number", "datatype": "str"},
            "SupplierCode": {"rename": "vendor_code", "datatype": "str"},
            "VendorName": {"rename": "vendor_name", "datatype": "str"},
            "Entity": {"rename": "entity_name", "datatype": "str"},
            "city_consolidated": {"rename": "city_name", "datatype": "str"},
            "FacilityId": {"rename": "facility_erp_id", "datatype": "str"},
            "FacilityName": {"rename": "facility_name", "datatype": "str"},
            "SkuCode": {"rename": "item_code", "datatype": "str"},
            "SkuDescription": {"rename": "product_name", "datatype": "str"},
            "BrandName": {"rename": "brand_name", "datatype": "str"},
            "CategoryId": {"rename": "category_path", "datatype": "str"},
            "OrderedQty": {"rename": "ordered_qty", "datatype": "float"},
            "ReceivedQty": {"rename": "received_qty", "datatype": "float"},
            "BalancedQty": {"rename": "pending_qty", "datatype": "float"},
            "UnitBasedCost": {"rename": "unit_base_cost", "datatype": "float"},
            "Mrp": {"rename": "product_mrp", "datatype": "float"},
            "PoLineValueWithoutTax": {"rename": "line_value_without_tax", "datatype": "float"},
            "PoLineValueWithTax": {"rename": "line_value_with_tax", "datatype": "float"},
            "PoAmount": {"rename": "po_header_amount", "datatype": "float"},
            "Tax": {"rename": "tax_amount", "datatype": "float"},
            "created_date": {"rename": "created_date", "datatype": "date"},
            "modified_date": {"rename": "modified_date", "datatype": "date"},
        }

    @classmethod
    def get_config_zepto(cls):
        # hangyo_zepto_po. SKU (UUID) is the product key; no category column:
        # category from the product. City = city_consolidated.
        return {
            "id": {"rename": "po_id", "datatype": "str"},
            "snapshot_date": {"rename": "snapshot_date", "datatype": "date"},
            "PO_No": {"rename": "po_number", "datatype": "str"},
            "Line_No": {"rename": "po_line_no", "datatype": "int"},
            "PO_Date": {"rename": "po_date", "datatype": "date"},
            "PO_Expiry_Date": {"rename": "po_expiry_date", "datatype": "date"},
            "Status": {"rename": "po_status_raw", "datatype": "str"},
            "Created_By": {"rename": "po_created_by", "datatype": "str"},
            "Vendor_Code": {"rename": "vendor_code", "datatype": "str"},
            "Vendor_Name": {"rename": "vendor_name", "datatype": "str"},
            "Del_Location": {"rename": "facility_name", "datatype": "str"},
            "city_consolidated": {"rename": "city_name", "datatype": "str"},
            "SKU": {"rename": "item_code", "datatype": "str"},
            "SKU_Desc": {"rename": "product_name", "datatype": "str"},
            "Brand": {"rename": "brand_name", "datatype": "str"},
            "EAN": {"rename": "product_ean", "datatype": "str"},
            "HSN": {"rename": "product_hsn", "datatype": "str"},
            "Qty": {"rename": "ordered_qty", "datatype": "float"},
            "GRN_Quantity": {"rename": "received_qty", "datatype": "float"},
            "ASN_Quantity": {"rename": "asn_qty", "datatype": "float"},
            "Unit_Base_Cost": {"rename": "unit_base_cost", "datatype": "float"},
            "Landing_Cost": {"rename": "unit_landing_cost", "datatype": "float"},
            "MRP_RSP": {"rename": "product_mrp", "datatype": "float"},
            "Total_Amount": {"rename": "line_value_with_tax", "datatype": "float"},
            "PO_Amount": {"rename": "po_header_amount", "datatype": "float"},
            "CGST_pct": {"rename": "cgst_pct", "datatype": "float"},
            "SGST_pct": {"rename": "sgst_pct", "datatype": "float"},
            "IGST_pct": {"rename": "igst_pct", "datatype": "float"},
            "CESS_pct": {"rename": "cess_pct", "datatype": "float"},
            "created_date": {"rename": "created_date", "datatype": "date"},
            "modified_date": {"rename": "modified_date", "datatype": "date"},
        }