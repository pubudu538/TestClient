/**
 * Created by maheshakya on 6/12/15.
 */

import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAgentConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAuthenticationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointException;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.exception.*;
import org.wso2.carbon.databridge.commons.utils.DataBridgeCommonsUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.*;
import java.util.Enumeration;
import java.util.Scanner;
import java.lang.String;


public class TestThriftClient {
    private static final String DATA_STREAM = "streaming_data";
    private static final String VERSION = "1.0.0";
    private static String SAMPLE_STREAM_PATH = System.getProperty("user.dir") + "/resources/sample_only_5.csv";
    private static int defaultThriftPort = 30135;
    private static int defaultBinaryPort = 30051;

    public static void main(String[] args) throws DataEndpointAuthenticationException,
            DataEndpointAgentConfigurationException,
            TransportException,
            DataEndpointException,
            DataEndpointConfigurationException,
            FileNotFoundException,
            SocketException,
            UnknownHostException {

        String host = getLocalAddress().getHostAddress();

        System.out.println(args);

        if (args.length == 0 )
        {
            System.out.println("Please provide - file location, receiver ip and port");
        }
        else
        {
            SAMPLE_STREAM_PATH = args[0];
            host = args[1];
            defaultThriftPort = Integer.parseInt(args[2]);

        }

        System.out.println("Starting stream");
        String currentDir = System.getProperty("user.dir");
        System.setProperty("javax.net.ssl.trustStore", currentDir + "client-truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "wso2carbon");

        AgentHolder.setConfigPath(getDataAgentConfigPath());

        String type = getProperty("type", "Thrift");
        int receiverPort = defaultThriftPort;
        if (type.equals("Binary")) {
            receiverPort = defaultBinaryPort;
        }
        int securePort = receiverPort + 1;

        String url = getProperty("url", "tcp://" + host + ":" + receiverPort);
        String authURL = getProperty("authURL", "ssl://" + host + ":" + securePort);
        String username = getProperty("username", "admin");
        String password = getProperty("password", "admin");

        DataPublisher dataPublisher = new DataPublisher(type, url, authURL, username, password);

        String streamId = DataBridgeCommonsUtils.generateStreamId(DATA_STREAM, VERSION);
        System.out.println(streamId);
        publishLogEvents(dataPublisher, streamId);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }

        dataPublisher.shutdown();
    }

    public static String getDataAgentConfigPath() {
        File filePath = new File("src" + File.separator + "resources");
        if (!filePath.exists()) {
            filePath = new File("test" + File.separator + "resources");
        }
        if (!filePath.exists()) {
            filePath = new File("resources");
        }
        if (!filePath.exists()) {
            filePath = new File("test" + File.separator + "resources");
        }
        return filePath.getAbsolutePath() + File.separator + "data-agent-conf.xml";
    }

    private static void publishLogEvents(DataPublisher dataPublisher, String streamId) throws FileNotFoundException {
        Scanner scanner = new Scanner(new FileInputStream(SAMPLE_STREAM_PATH));
        int i = 1;
        while (scanner.hasNextLine()) {
            System.out.println("Publish streaming event : " + i);
            String anEntry = scanner.nextLine();
            String[] separatedEntries = anEntry.split(",");
            Object[] payload = new Object[]{
                    Integer.parseInt(separatedEntries[0]), Integer.parseInt(separatedEntries[1]),
                    Float.parseFloat(separatedEntries[2]), Boolean.parseBoolean(separatedEntries[3]),
                    Integer.parseInt(separatedEntries[4]), Integer.parseInt(separatedEntries[5]),
                    separatedEntries[6]
                    };
            Event event = new Event(streamId, System.currentTimeMillis(), new Object[]{"electricityConsumption"}, null,
                    payload);
            dataPublisher.publish(event);
            i++;
        }

        scanner.close();
    }

    public static InetAddress getLocalAddress() throws SocketException, UnknownHostException {
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while (ifaces.hasMoreElements()) {
            NetworkInterface iface = ifaces.nextElement();
            Enumeration<InetAddress> addresses = iface.getInetAddresses();

            while (addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                if (addr instanceof Inet4Address && !addr.isLoopbackAddress()) {
                    return addr;
                }
            }
        }
        return InetAddress.getLocalHost();
    }


    private static String getProperty(String name, String def) {
        String result = System.getProperty(name);
        if (result == null || result.length() == 0 || result == "") {
            result = def;
        }
        return result;
    }

}
