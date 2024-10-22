weights_composite_stability_score = {
            'overall_usage_score': 0.2,
            'overall_incident_score': 0.5,
            'maintenance_score': 0.3,
            'vulnerability_score': 0.5
        }
        
severity_mapping = {
            '1 - high': 10, 'high': 10,
            '2 - medium': 4, 'medium': 4,
            '3 - low': 0.5, 'low': 0.5
        }
impact_mapping = {
            '1 - high': 10, 'high': 10,
            '2 - medium': 4, 'medium': 4,
            '3 - low': 0.5, 'low': 0.5
        }

# Azure Blob Storage setup
source_folder_name = 'AssetFiles'

target_folder_name = 'AssetFiles_With_Scores'

csv_report_asset_with_risk_file_location ='Reports/csv_report/summarized_asset_scores_with_risk_category.csv'

pdf_report_file_location = "Reports/pdf_report/High Risk Hardware Report.pdf"

code_string_file_name = 'AssetFiles_With_Scores/complete_code_string.txt'