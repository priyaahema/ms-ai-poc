import logging
import azure.functions as func
from HighRisk_Analysis.HWDataPreprocessing import DataProcessorClass
from HighRisk_Analysis.ReportGenerator import ReportGeneratorClass

app = func.FunctionApp()

@app.schedule(schedule="0 9 * * 1", arg_name="myTimer", run_on_startup=True,use_monitor=False) 
def AI_MS_HW_REPORT(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    processor = DataProcessorClass()
    processor.main()
    logging.info('Data Processing Executed....')

    report_generator = ReportGeneratorClass()
    report_generator.main()
    logging.info('Report Generation Executed...')
    

# HTTP Trigger Function
@app.function_name(name="AI_MS_HTTP_REPORT")
@app.route(route="generate_report", methods=["GET", "POST"])
def generate_report(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('HTTP request received to generate report...')
        
        # Execute data processing
        processor = DataProcessorClass()
        processor.main()
        logging.info('Data Processing Executed....')

        # Execute report generation
        # report_generator = ReportGeneratorClass()
        # report_generator.main()
        # logging.info('Report Generation Executed...')

        return func.HttpResponse("Report generation completed successfully.", status_code=200)
    
    except Exception as e:
        logging.error(f"Error generating report: {str(e)}")
        return func.HttpResponse(f"Failed to generate report: {str(e)}", status_code=500)