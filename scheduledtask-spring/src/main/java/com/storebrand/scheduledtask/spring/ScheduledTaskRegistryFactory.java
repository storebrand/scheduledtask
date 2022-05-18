package com.storebrand.scheduledtask.spring;

import java.time.Clock;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.storebrand.scheduledtask.ScheduledTaskRegistry;
import com.storebrand.scheduledtask.ScheduledTaskRegistryImpl;
import com.storebrand.scheduledtask.db.MasterLockRepository;
import com.storebrand.scheduledtask.db.ScheduledTaskRepository;
import com.storebrand.scheduledtask.db.sql.MasterLockSqlRepository;
import com.storebrand.scheduledtask.db.sql.ScheduledTaskSqlRepository;

/**
 * Factory bean that will create an implementation of {@link ScheduledTaskRegistry}. This will also handle shutting down
 * the service when the application context is shutting down.
 * <p>
 * {@link ScheduledTaskRegistryImpl} requires both a {@link ScheduledTaskRepository} and a {@link MasterLockRepository}.
 * The factory will try to inject these as optional dependencies. If they are found it will use them.
 * <p>
 * If there are no repositories present the factory will look for a {@link DataSource} in the Spring context, and use
 * that to create the default implementations of the repositories. Note that default implementations created here will
 * not be available through the Spring context, as they are only meant for internal use. Also, if there are multiple
 * datasources available in the Spring context the primary bean will be used. If there are no primary bean
 * initialization will fail.
 * <p>
 * As the service and repositories also require a {@link Clock} the factory will look for that in the Spring context as
 * well. If it is not found {@link Clock#systemDefaultZone()} will be used.
 *
 * @author Kristian Hiim
 */
public class ScheduledTaskRegistryFactory extends AbstractFactoryBean<ScheduledTaskRegistry>
        implements ApplicationContextAware {
    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskRegistryFactory.class);

    @Inject
    private Optional<MasterLockRepository> _masterLockRepository;

    @Inject
    private Optional<ScheduledTaskRepository> _scheduledTaskRepository;

    @Inject
    private Optional<List<DataSource>> _dataSources;

    @Inject
    private Optional<Clock> _clock;

    private ApplicationContext _applicationContext;

    public ScheduledTaskRegistryFactory() {
        setSingleton(true);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        _applicationContext = applicationContext;
    }

    @Override
    public Class<?> getObjectType() {
        return ScheduledTaskRegistryImpl.class;
    }

    protected Clock getClock() {
        return _clock.orElseGet(Clock::systemDefaultZone);
    }

    @Override
    protected ScheduledTaskRegistry createInstance() {
        // Use Clock from Spring context if it is present, or use system default zone.
        final Clock clock = getClock();

        // Attempt to detect if we have a single DataSource in Spring context.
        // dataSource will be empty if there are 0 or more than 1 datasource.
        final Optional<DataSource> dataSource = _dataSources.isPresent() && _dataSources.get().size() == 1
                ? Optional.of(_dataSources.get().get(0))
                : Optional.empty();

        // Get MasterLockRepository from Spring context, or create a default implementation if not present.
        MasterLockRepository masterLockRepository = _masterLockRepository.orElseGet(() -> {
            log.info(MasterLockRepository.class.getSimpleName() + " not found in Spring context - Creating default implementation "
                    + MasterLockSqlRepository.class.getName());
            return new MasterLockSqlRepository(
                    dataSource.orElseGet(() -> _applicationContext.getBean(DataSource.class)),
                    clock);
        });

        // Get ScheduledTaskRepository from Spring context, or create a default implementation if not present.
        ScheduledTaskRepository scheduledTaskRepository = _scheduledTaskRepository.orElseGet(() -> {
            log.info(ScheduledTaskRepository.class.getSimpleName() + " not found in Spring context - Creating default implementation "
                    + ScheduledTaskSqlRepository.class.getName());
            return new ScheduledTaskSqlRepository(
                    dataSource.orElseGet(() -> _applicationContext.getBean(DataSource.class)),
                    clock);
        });

        return new ScheduledTaskRegistryImpl(scheduledTaskRepository, masterLockRepository, clock,
                TestModeUtil.isTestMode());
    }

    @Override
    protected void destroyInstance(ScheduledTaskRegistry instance) {
        if (instance != null) {
            instance.close();
        }
    }
}
