package pt.ulisboa.depchain.shared.network.links;

import static pt.ulisboa.depchain.shared.validation.ValidationUtils.named;

import java.util.concurrent.atomic.AtomicReference;

import pt.ulisboa.depchain.shared.validation.ValidationUtils;

public final class RunOnce implements Runnable {
  private final AtomicReference<Runnable> action;

  public RunOnce(Runnable action) {
    ValidationUtils.requireAllNonNull(named("action", action));
    this.action = new AtomicReference<>(action);
  }

  @Override
  public void run() {
    Runnable currentAction = action.getAndSet(null);
    if (currentAction != null) {
      currentAction.run();
    }
  }
}
