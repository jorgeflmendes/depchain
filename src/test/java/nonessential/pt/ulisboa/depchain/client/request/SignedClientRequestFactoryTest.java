package pt.ulisboa.depchain.client.request;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;

import org.junit.jupiter.api.Test;

import pt.ulisboa.depchain.proto.TransactionType;
import pt.ulisboa.depchain.shared.crypto.ClientRequestSignaturePayloadUtil;
import pt.ulisboa.depchain.shared.crypto.CryptoUtil;

class SignedClientRequestFactoryTest {
  private static final long CLIENT_SENDER_ID = 100L;
  private static final String RECIPIENT = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  @Test
  void createTransactionRequestPreservesIstCoinTransferTypeAndSignaturePayload() throws Exception {
    KeyPair clientKeyPair = CryptoUtil.createEcKeyPair();
    SignedClientRequestFactory factory = new SignedClientRequestFactory(CLIENT_SENDER_ID, clientKeyPair.getPrivate());

    var request = factory.createTransactionRequest(TransactionType.TRANSACTION_TYPE_IST_COIN_TRANSFER, RECIPIENT, 25L, 7L, 250_000L, 3L, new byte[0]);

    assertTrue(request.hasTransaction());
    assertEquals(TransactionType.TRANSACTION_TYPE_IST_COIN_TRANSFER, request.getTransaction().getType());
    assertEquals(RECIPIENT, request.getTransaction().getTo());
    assertEquals(25L, request.getTransaction().getAmount());
    assertEquals(7L, request.getTransaction().getNonce());
    assertEquals(250_000L, request.getTransaction().getGasLimit());
    assertEquals(3L, request.getTransaction().getGasPrice());

    byte[] expectedPayload = ClientRequestSignaturePayloadUtil.signedTransactionRequestPayload(CLIENT_SENDER_ID, request.getTransaction().getRequestKey()
        .getRequestId(), TransactionType.TRANSACTION_TYPE_IST_COIN_TRANSFER, RECIPIENT, 25L, 7L, 250_000L, 3L, new byte[0]);
    assertTrue(CryptoUtil.verifyEcdsa(expectedPayload, request.getTransaction().getSignature().toByteArray(), clientKeyPair.getPublic()));
    assertArrayEquals(new byte[0], request.getTransaction().getInput().toByteArray());
  }
}
