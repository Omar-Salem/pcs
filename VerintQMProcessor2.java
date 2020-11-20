import com.dimensiondata.pcs.bean.NuanceHelperBean;
import com.dimensiondata.pcs.exception.CallNotTaggedException;
import com.dimensiondata.pcs.exception.NullMandatoryException;
import com.dimensiondata.pcs.helper.SystemPropertyLoader;
import com.dimensiondata.pcs.processor.VerintQMProcessor;
import com.dimensiondata.pcs.ws.VerintQMPortal;
import com.dimensiondata.pcs.ws.VerintQMResponseParser;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.xml.soap.SOAPMessage;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.log4j.Logger;

public class VerintQMProcessor implements Processor {
  private static Logger log = Logger.getLogger(VerintQMProcessor.class.getName());
  
  private String name;
  
  private int queryPeriodInHours;
  
  private SimpleDateFormat reportDateFormat;
  
  private SimpleDateFormat verintQMDateFormat;
  
  private SimpleDateFormat crmDateFormat;
  
  private SimpleDateFormat dateOnlyDateFormat;
  
  private VerintQMPortal verintQMPortal;
  
  private VerintQMResponseParser verintQMResponseParser;
  
  private String serverURI;
  
  private String verintCRMQueryURL;
  
  private boolean isForReportingModule;
  
  private static Date timeOfLastVerintResponse;
  
  public static final String EMPLOYEENUMBER = "employeeNumber";
  
  public static final String EVENTCAPTUREDATE = "eventCaptureDate";
  
  public static final String EVENTDURATIONCOUNT = "contact_duration_seconds";
  
  public static final String CUSTOMERNUMBER = "customerNumber";
  
  public static final String CALLRECORDINGURI = "callRecordingURI";
  
  public VerintQMProcessor(String theName, boolean isForReportingModule) throws Exception {
    log.info("VerintQMProcessor " + theName);
    this.name = theName;
    this.isForReportingModule = isForReportingModule;
    this.serverURI = SystemPropertyLoader.getSipPortalProperties().getProperty("Verint.ServerURI");
    String soapUrl = this.serverURI + "DAS101/AgWebService.asmx";
    String verintQMUserId = SystemPropertyLoader.getSipPortalProperties().getProperty("Verint.QM.UserId");
    String ntlmUsername = SystemPropertyLoader.getSipPortalProperties().getProperty("NTLM.Username");
    String ntlmPassword = SystemPropertyLoader.getSipPortalProperties().getProperty("NTLM.Password");
    String ntlmDomain = SystemPropertyLoader.getSipPortalProperties().getProperty("NTLM.Domain");
    String queryPeriodInHoursS = SystemPropertyLoader.getSipPortalProperties().getProperty("Verint.QM.QueryPeriodInHours");
    this.verintCRMQueryURL = SystemPropertyLoader.getSipPortalProperties().getProperty("Verint.CRMQuery.URL");
    this.queryPeriodInHours = Integer.parseInt(queryPeriodInHoursS);
    this.verintQMPortal = new VerintQMPortal(verintQMUserId, soapUrl, ntlmUsername, ntlmPassword, ntlmDomain);
    Map<Object, Object> xpaths = new HashMap<>();
    xpaths.put("AUDIO_START_TIME", "/soap:Envelope/soap:Body/dft:ExecuteSessionQueryResponse/dft:ExecuteSessionQueryResult/diffgr:diffgram/NewDataSet/Sessions[%d]//.[lower-case(name())='audio_start_time']");
    xpaths.put("pbx_login_id", "/soap:Envelope/soap:Body/dft:ExecuteSessionQueryResponse/dft:ExecuteSessionQueryResult/diffgr:diffgram/NewDataSet/Sessions[%d]/pbx_login_id");
    xpaths.put("contact_duration_seconds", "/soap:Envelope/soap:Body/dft:ExecuteSessionQueryResponse/dft:ExecuteSessionQueryResult/diffgr:diffgram/NewDataSet/Sessions[%d]/contact_duration_seconds");
    xpaths.put("audio_end_time", "/soap:Envelope/soap:Body/dft:ExecuteSessionQueryResponse/dft:ExecuteSessionQueryResult/diffgr:diffgram/NewDataSet/Sessions[%d]/audio_end_time");
    xpaths.put("cd3", "/soap:Envelope/soap:Body/dft:ExecuteSessionQueryResponse/dft:ExecuteSessionQueryResult/diffgr:diffgram/NewDataSet/Sessions[%d]/cd3");
    xpaths.put("cd4", "/soap:Envelope/soap:Body/dft:ExecuteSessionQueryResponse/dft:ExecuteSessionQueryResult/diffgr:diffgram/NewDataSet/Sessions[%d]/cd4");
    this.verintQMResponseParser = new VerintQMResponseParser(xpaths);
    this.reportDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    this.verintQMDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS");
    this.crmDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    this.crmDateFormat = new SimpleDateFormat("yyyy-MM-dd%20HH:mm:ss");
    this.dateOnlyDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  }
  
  public void process(Exchange exchange) throws Exception {
    HashMap<String, String> hashMap = (HashMap<String, String>)exchange.getIn().getBody();
    log.debug(String.format("------ Start VerintQMProcessor %s ----", new Object[] { this.name }));
    String connid = hashMap.get("sourceSystemIdentifier");
    log.debug(String.format("connid :%s:", new Object[] { connid }));
    String surveyStartDate = hashMap.get("RESPONSESTARTDATEAOTZ");
    log.debug(String.format("process(): surveyStartDate :%s:", new Object[] { surveyStartDate }));
    String customerFeedbackDate = convertDateFormat(this.reportDateFormat, this.dateOnlyDateFormat, surveyStartDate, 0);
    hashMap.put("customerFeedbackDate", customerFeedbackDate);
    hashMap.put("verintqmstatus", NuanceHelperBean.FAILQMENRICH.toString());
    Date queryEndDate = stringToDate(this.reportDateFormat, surveyStartDate, 0);
    Date queryStartDate = stringToDate(this.reportDateFormat, surveyStartDate, -this.queryPeriodInHours);
    String verintQmQueryStartDate = this.verintQMDateFormat.format(queryStartDate);
    String verintQmQueryEndDateS = this.verintQMDateFormat.format(queryEndDate);
    String GUID = "01234567-1234-1234-1234-012345678901";
    SOAPMessage soapRequest = this.verintQMPortal.createExecuteSessionQuerySOAPMessage(GUID, this.serverURI, connid, verintQmQueryStartDate, verintQmQueryEndDateS);
    String requestXML = this.verintQMPortal.soapMessageToXML(soapRequest);
    log.debug(String.format("Request SOAP Message: %s", new Object[] { requestXML }));
    SOAPMessage soapResponse = this.verintQMPortal.soapCall(soapRequest);
    String responseXML = this.verintQMPortal.soapMessageToXML(soapResponse);
    log.debug(String.format("Response SOAP Message: %s", new Object[] { responseXML }));
    Map<String, String> res = this.verintQMResponseParser.parseResponse(responseXML, connid);
    timeOfLastVerintResponse = new Date();
    String employeeNumber = res.get("pbx_login_id");
    if (employeeNumber == null) {
      String msg = String.format("pbx_login_id not found in VerintQM for connid %s", new Object[] { connid });
      if (!this.isForReportingModule)
        throw new CallNotTaggedException(msg); 
    } 
    String AUDIO_START_TIME = res.get("AUDIO_START_TIME");
    if (AUDIO_START_TIME == null) {
      String msg = String.format("AUDIO_START_TIME not found in VerintQM for connid %s", new Object[] { connid });
      if (!this.isForReportingModule)
        throw new NullMandatoryException(msg); 
    } 
    String audio_end_time = res.get("audio_end_time");
    if (audio_end_time == null) {
      String msg = String.format("audio_end_time not found in VerintQM for connid %s", new Object[] { connid });
      if (!this.isForReportingModule)
        throw new NullMandatoryException(msg); 
    } 
    String contact_duration_seconds = res.get("contact_duration_seconds");
    if (contact_duration_seconds == null) {
      log.debug(String.format("No contact_duration_seconds found for connid %s", new Object[] { connid }));
      contact_duration_seconds = "-1";
    } 
    String customerNumber = res.get("cd4");
    if (customerNumber == null) {
      log.debug(String.format("No customer number (cd4) found for connid %s", new Object[] { connid }));
      customerNumber = "";
    } 
    hashMap.put("employeeNumber", employeeNumber);
    String eventCaptureDate = convertDateFormat(this.verintQMDateFormat, this.dateOnlyDateFormat, AUDIO_START_TIME, 0);
    hashMap.put("eventCaptureDate", eventCaptureDate);
    hashMap.put("contact_duration_seconds", contact_duration_seconds);
    hashMap.put("customerNumber", customerNumber);
    String crmAudioStartTime = convertDateFormat(this.verintQMDateFormat, this.crmDateFormat, AUDIO_START_TIME, 0);
    String crmAudioEndTime = convertDateFormat(this.verintQMDateFormat, this.crmDateFormat, audio_end_time, 0);
    String callRecordingURI = String.format("%s?Stime=%s&ETime=%s&Cd3=%s&world=etm", new Object[] { this.verintCRMQueryURL, crmAudioStartTime, crmAudioEndTime, connid });
    log.debug(String.format("process(): callRecordingURI = :%s:", new Object[] { callRecordingURI }));
    hashMap.put("callRecordingURI", callRecordingURI);
    hashMap.put("verintqmstatus", NuanceHelperBean.SUCCESSQMENRICH.toString());
    log.debug(String.format("------ End VerintQMProcessor %s ----", new Object[] { this.name }));
  }
  
  public static Date stringToDate(SimpleDateFormat sdf, String surveyStartDate, int queryPeriodInHours) throws ParseException {
    Date d = sdf.parse(surveyStartDate);
    Calendar c = Calendar.getInstance();
    c.setTime(d);
    c.add(11, queryPeriodInHours);
    return c.getTime();
  }
  
  public static String convertDateFormat(SimpleDateFormat sourceDateFormat, SimpleDateFormat targetDateFormat, String theDate, int queryPeriodInHours) throws ParseException {
    log.debug(String.format("convertDateFormat(): theDate :%s:, queryPeriodInHours :%d:", new Object[] { theDate, Integer.valueOf(queryPeriodInHours) }));
    if (theDate == null) {
      log.warn("No date supplied");
      return targetDateFormat.format(new Date(0L));
    } 
    Date d = sourceDateFormat.parse(theDate);
    if (queryPeriodInHours != 0) {
      Calendar c = Calendar.getInstance();
      c.setTime(d);
      c.add(11, queryPeriodInHours);
      d = c.getTime();
    } 
    return targetDateFormat.format(d);
  }
  
  public static Date getTimeOfLastVerintResponse() {
    return timeOfLastVerintResponse;
  }
}
