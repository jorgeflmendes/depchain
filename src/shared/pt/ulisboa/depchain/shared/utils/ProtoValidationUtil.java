package pt.ulisboa.depchain.shared.utils;

import com.google.protobuf.Message;

import build.buf.protovalidate.ValidationResult;
import build.buf.protovalidate.Validator;
import build.buf.protovalidate.ValidatorFactory;
import build.buf.protovalidate.exceptions.ValidationException;

public final class ProtoValidationUtil {
  private static final Validator VALIDATOR = ValidatorFactory.newBuilder().build();

  private ProtoValidationUtil() {
  }

  public static <T extends Message> T requireValid(T message, String label) {
    ValidationUtils.requireNonNull(message, "message");
    ValidationUtils.requireNonNull(label, "label");

    try {
      ValidationResult validationResult = VALIDATOR.validate(message);
      if (!validationResult.isSuccess()) {
        throw new IllegalArgumentException(label + " failed protovalidate: " + validationResult);
      }
      return message;
    } catch (ValidationException exception) {
      throw new IllegalStateException("Unable to validate " + label, exception);
    }
  }
}
