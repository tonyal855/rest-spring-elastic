package rest.springelastic.restspringelastic.conf;

import java.net.InetAddress;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {

	@Bean
	public Client client() throws Throwable {

	Settings settings = Settings.builder().put("cluster.name", "elasticIMM").build();

	PreBuiltTransportClient preBuiltTransportClient = new PreBuiltTransportClient(settings);
	TransportClient client = preBuiltTransportClient
	.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.180.133"), 9900))
	.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.180.148"), 9900))
	.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.180.145"), 9900))
	.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.180.147"), 9900));
	return client;
	}

	
}
