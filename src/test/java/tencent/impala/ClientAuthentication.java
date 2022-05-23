package tencent.impala;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * Description:
 *
 * @author zz
 * @date 2022/5/23
 */
public class ClientAuthentication {
    public static void main(String[] args) throws Exception {
        String username = "admin";
        String password = "admin";
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                new AuthScope("192-168-0-3", AuthScope.ANY_PORT),
                new UsernamePasswordCredentials(username, password));
        CloseableHttpClient httpclient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build();
        try {
            HttpGet httpget = new HttpGet("http://192-168-0-3:7180/cmf/yarn/completedApplications?startTime=1653288428511&endTime=1653290230772&filters=hive_query_id%20RLIKE%20%22.*%22&offset=0&limit=100&serviceName=yarn&histogramAttributes=adl_bytes_read%2Cadl_bytes_written%2Ccpu_milliseconds%2Cs3a_bytes_read%2Cs3a_bytes_written%2Cused_memory_max%2Cmb_millis%2Chdfs_bytes_written%2Cfile_bytes_written%2Callocated_vcore_seconds%2Callocated_memory_seconds%2Ctotal_launched_tasks%2Capplication_duration%2Cunused_vcore_seconds%2Cunused_memory_seconds%2Cpool%2Cuser%2Chdfs_bytes_read%2Cfile_bytes_read&_=1653290228699");

            System.out.println("Executing request " + httpget.getRequestLine());
            CloseableHttpResponse response = httpclient.execute(httpget);
            try {
                System.out.println("----------------------------------------");
                System.out.println(response.getStatusLine());
                System.out.println(EntityUtils.toString(response.getEntity()));
            } finally {
                response.close();
            }
        } finally {
            httpclient.close();
        }
    }
}
