package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.ses.SesClient;
import software.amazon.awssdk.services.ses.model.*;

import java.io.IOException;

public class OrderProcessorHandler implements RequestHandler<SQSEvent, Void> {

    private final SnsClient snsClient = SnsClient.builder()
            .region(Region.of("sa-east-1"))
            .build();

    private final SesClient sesClient = SesClient.builder()
            .region(Region.of("sa-east-1"))
            .build();

    private static final String SNS_TOPIC_ARN = System.getenv("SNS_TOPIC_ARN");

    private static final String EMAIL_SENDER = System.getenv("EMAIL_SENDER");
    private static final String EMAIL_RECIPIENT = System.getenv("EMAIL_RECIPIENT");

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        for (SQSEvent.SQSMessage msg : event.getRecords()) {
            String orderJson = msg.getBody();
            System.out.println("Procesando pedido: " + orderJson);

            Order order = parseOrder(orderJson);

            // Enviar a través de SNS
            sendSnsNotification(order);

            // Enviar el correo con los detalles del pedido a través de SES
            sendEmailNotification(order);
        }
        return null;
    }


    private Order parseOrder(String orderJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(orderJson, Order.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void sendSnsNotification(Order order) {
        if (SNS_TOPIC_ARN == null || SNS_TOPIC_ARN.isEmpty()) {
            System.err.println("Error: SNS_TOPIC_ARN no está configurado correctamente.");
            return;
        }

        String message = String.format("Pedido recibido: ID=%s, Producto=%s, Cantidad=%d, Total=%.2f",
                order.getOrderId(), order.getProductId(), order.getQuantity(), order.getTotalPrice());

        PublishRequest publishRequest = PublishRequest.builder()
                .topicArn(SNS_TOPIC_ARN)
                .message(message)
                .build();

        PublishResponse response = snsClient.publish(publishRequest);
        System.out.println("SMS enviado. MessageId: " + response.messageId());
    }

    private void sendEmailNotification(Order order) {
        String subject = "Nuevo Pedido Recibido: " + order.getOrderId();

        String htmlBody = String.format(
                "<html>" +
                        "<body>" +
                        "<h1>Nuevo Pedido</h1>" +
                        "<p><strong>Order ID:</strong> %s</p>" +
                        "<p><strong>Producto:</strong> %s</p>" +
                        "<p><strong>Cantidad:</strong> %d</p>" +
                        "<p><strong>Total:</strong> %.2f</p>" +
                        "<p><strong>Cliente:</strong> %s</p>" +
                        "<p><strong>Fecha de Creación:</strong> %s</p>" +
                        "</body>" +
                        "</html>",
                order.getOrderId(),
                order.getProductId(),
                order.getQuantity(),
                order.getTotalPrice(),
                order.getCustomerName(),
                order.getCreatedAt());

        SendEmailRequest sendEmailRequest = SendEmailRequest.builder()
                .destination(Destination.builder()
                        .toAddresses(EMAIL_RECIPIENT)
                        .build())
                .message(Message.builder()
                        .subject(Content.builder().data(subject).build())
                        .body(Body.builder()
                                .html(Content.builder().data(htmlBody).build())
                                .build())
                        .build())
                .source(EMAIL_SENDER)
                .build();

        sesClient.sendEmail(sendEmailRequest);
        System.out.println("Correo enviado con éxito.");
    }
}
