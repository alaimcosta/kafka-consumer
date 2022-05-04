package unifesspa.kafka.consumer.eventos;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


//implementar o consumidor
public class ConsumidorEvento {
	
	private final KafkaConsumer<String, String> consumer;
	
	public ConsumidorEvento() {
		consumer = criarConsumer();
		
	}
	
	private KafkaConsumer<String, String> criarConsumer(){
		
		if(consumer != null) {
			return consumer;
		}
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092"); //servidor do kafka
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //Processo inverso de deserialização
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//Processo inverso de deserialização
        properties.put("group.id", "default");//Quantidade de consumidores que vão receber a mesma mensagem, 
		
        //Crio o consumidor
		return new KafkaConsumer<String, String>(properties);
		
		
	}
	
	
	
	public void executar() {
		//Topicos da aplicação
		java.util.List<String> topicos = new ArrayList<>();
		topicos.add("RegistroEvento"); //adiciono o topico, deve estar com o mesmo nome do Offset
		//Se inscrever no topico
		consumer.subscribe(topicos);
		
		System.out.println("Iniciando consumer...");
		//aqui vou consumir as mensagens
		
		
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));//de quanto tem vou atualizar
			for(ConsumerRecord<String, String> record : records) { //caso receba mais de uma mensagem
				gravarMensagem(record.topic(), record.partition(), record.value());
			}
		}
	}
	
	private void gravarMensagem(String topico, int particao, String msg) { //recebo topico, partição e a mensagem
		System.out.println("Topico:"+ topico + "Partição: "+ particao+ "Mensagem: "+ msg);
	}

}
