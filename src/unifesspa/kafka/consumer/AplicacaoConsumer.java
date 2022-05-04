package unifesspa.kafka.consumer;

import unifesspa.kafka.consumer.eventos.ConsumidorEvento;

public class AplicacaoConsumer {
	public static void main(String[] args) {
		AplicacaoConsumer aplicacao = new AplicacaoConsumer();
		aplicacao.iniciar();
	}
	
	private void iniciar() {
		System.out.println("Iniciando apliçao!");
		ConsumidorEvento consumidor = new ConsumidorEvento();
		consumidor.executar();
	}

}
