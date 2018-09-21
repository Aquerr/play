package schedulers;

import actors.DocumentActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import play.Logger;
import scala.compat.java8.FutureConverters;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.Duration;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static akka.pattern.Patterns.ask;

@Singleton
public class DocumentScheduler
{
    private static final Logger.ALogger LOGGER = Logger.of(DocumentScheduler.class);

    private final ActorRef removerActor;
    private final ActorSystem actorSystem;
    private final ExecutionContext executionContext;

    @Inject
    public DocumentScheduler(ActorSystem actorSystem, ExecutionContext executionContext)
    {
        this.removerActor = actorSystem.actorOf(DocumentActor.getProps());
        this.actorSystem = actorSystem;
        this.executionContext = executionContext;

        LOGGER.warn("Initializing DocumentScheduler...");
        //initialize();
        scheduleRemoveTask();
    }

    private void scheduleRemoveTask()
    {
        this.actorSystem.scheduler().schedule(
                Duration.create(0, TimeUnit.SECONDS),
                Duration.create(10, TimeUnit.SECONDS),
                removerActor,
                "RemoveDocuments",
                executionContext,
                ActorRef.noSender()
        );
    }

    private void initialize()
    {
        sayHello("Bartek").thenAccept(new Consumer<Object>()
        {

            @Override
            public void accept(Object o)
            {
                LOGGER.warn("Got " + o.toString());
            }
        });


    }

    public CompletionStage<Object> sayHello(String name)
    {
        return FutureConverters.toJava(ask(removerActor, "Hello" + name + "!", 1000));
    }
}
