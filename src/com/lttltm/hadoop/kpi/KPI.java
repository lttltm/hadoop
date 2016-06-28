package com.lttltm.hadoop.kpi;

import java.util.HashSet;
import java.util.Set;

/*
 ::1 - - [26/Jun/2016:10:32:58 +0800] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/50.0.2661.102 Chrome/50.0.2661.102 Safari/537.36"

 */

public class KPI {

	private String remote_addr;// ��¼�ͻ��˵�ip��ַ
	private String remote_user;// ��¼�ͻ����û�����,��������"-"
	private String time_local;// ��¼����ʱ����ʱ��
	private String request_method;
	private String request_url;// ��¼�����url��httpЭ��
	private String status;// ��¼����״̬���ɹ���200
	private String body_bytes_sent;// ��¼���͸��ͻ����ļ��������ݴ�С
	private String http_referer;// ������¼���Ǹ�ҳ�����ӷ��ʹ�����
	private String http_user_agent;// ��¼�ͻ�������������Ϣ

	private boolean valid = true;// �ж������Ƿ�Ϸ�

	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}

	public void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}

	public void setTime_local(String time_local) {
		this.time_local = time_local;
	}


	public void setStatus(String status) {
		this.status = status;
	}

	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}

	public void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}

	public void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}

	public String getRemote_addr() {
		return remote_addr;
	}

	public String getRemote_user() {
		return remote_user;
	}

	public String getTime_local() {
		return time_local;
	}


	public String getRequest_method() {
		return request_method;
	}

	public void setRequest_method(String request_method) {
		this.request_method = request_method;
	}

	public String getRequest_url() {
		return request_url;
	}

	public void setRequest_url(String request_url) {
		this.request_url = request_url;
	}

	public String getStatus() {
		return status;
	}

	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}

	public String getHttp_referer() {
		return http_referer;
	}

	public String getHttp_user_agent() {
		return http_user_agent;
	}

	public boolean isValid() {
		return valid;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("valid:" + this.valid);
		sb.append("\nremote_addr:" + this.remote_addr);
		sb.append("\nremote_user:" + this.remote_user);
		sb.append("\ntime_local:" + this.time_local);
		sb.append("\nrequest_method:" + this.request_method);
		sb.append("\nrequest_url:" + this.request_url);
		sb.append("\nstatus:" + this.status);
		sb.append("\nbody_bytes_sent:" + this.body_bytes_sent);
		sb.append("\nhttp_referer:" + this.http_referer);
		sb.append("\nhttp_user_agent:" + this.http_user_agent);
		return sb.toString();
	}
	
	
	private static KPI parser(String line) {
        System.out.println(line);
        KPI kpi = new KPI();
        String[] arr = line.split(" ");
        if (arr.length > 11) {
            kpi.setRemote_addr(arr[0]);
            kpi.setRemote_user(arr[1]);
            kpi.setTime_local(arr[3].substring(1));
            kpi.setRequest_method(arr[5]);
            kpi.setRequest_url(arr[6]);
            kpi.setStatus(arr[8]);
            kpi.setBody_bytes_sent(arr[9]);
            kpi.setHttp_referer(arr[10]);
            
            if (arr.length > 12) {
                kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
            } else {
                kpi.setHttp_user_agent(arr[11]);
            }

            if (Integer.parseInt(kpi.getStatus()) >= 400) {// ����400��HTTP����
                kpi.setValid(false);
            }
        } else {
            kpi.setValid(false);
        }
        return kpi;
    }
	
	 /**
     * ��page��pv����
     */
    public static KPI filterPVs(String line) {
        KPI kpi = parser(line);
        Set<String> pages = new HashSet<String>();
        pages.add("/");
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");

        if (!pages.contains(kpi.getRequest_url())) {
            kpi.setValid(false);
        }
        return kpi;
    }

	public static void main(String[] args) {

		String line = "::1 - - [26/Jun/2016:10:32:58 +0800] \"GET / HTTP/1.1\" 304 0 \"-\" \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/50.0.2661.102 Chrome/50.0.2661.102 Safari/537.36\"";

		KPI kpi = new KPI();
		String[] arr = line.split(" ");

		kpi.setRemote_addr(arr[0]);
		kpi.setRemote_user(arr[1]);
		kpi.setTime_local(arr[3].substring(1));
		kpi.setRequest_method(arr[5]);
        kpi.setRequest_url(arr[6]);
		kpi.setStatus(arr[8]);
		kpi.setBody_bytes_sent(arr[9]);
		kpi.setHttp_referer(arr[10]);
		kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
		System.out.println(kpi);
	}
}
