package io.github.excalibase.service;

import io.github.excalibase.annotation.ExcalibaseService;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Service lookup utility for database-specific implementation resolution.
 *
 * <p>This service provides dynamic lookup functionality to resolve database-specific
 * implementations based on service names defined in {@link ExcalibaseService} annotations.
 * It enables the framework to support multiple database types by automatically selecting
 * the appropriate implementation at runtime.</p>
 *
 * <p>The lookup mechanism works by:</p>
 * <ol>
 *   <li>Scanning all beans of the requested service type</li>
 *   <li>Filtering beans by their {@link ExcalibaseService#serviceName()}</li>
 *   <li>Returning the matching implementation or throwing an exception</li>
 * </ol>
 *
 * <p>Example usage:</p>
 * <pre>
 * {@code
 * IValidationService validationService = serviceLookup.forBean(
 *     IValidationService.class,
 *     "postgres"
 * );
 * }
 * </pre>
 *
 * @see ExcalibaseService
 * @see ApplicationContext
 */
@Service
public class ServiceLookup {

    /** Spring application context for bean lookup operations */
    private final ApplicationContext applicationContext;

    /**
     * Constructs a new ServiceLookup with the provided application context.
     *
     * @param applicationContext the Spring application context for bean resolution
     */
    public ServiceLookup(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    /**
     * Finds and returns a service implementation by type and component name.
     *
     * <p>This method searches for all beans of the specified service class and filters
     * them by the {@link ExcalibaseService#serviceName()} annotation value. It returns
     * the first matching implementation or throws an exception if none is found.</p>
     *
     * @param <T> the service interface type to lookup
     * @param serviceClass the class of the service interface to find implementations for
     * @param componentType the service name to match against {@link ExcalibaseService#serviceName()}
     * @return the matching service implementation instance
     * @throws NoSuchBeanDefinitionException if no implementation is found for the given
     *                                      service class and component type combination
     */
    public <T> T forBean(Class<T> serviceClass, String componentType) {

        Map<String, T> beans = applicationContext.getBeansOfType(serviceClass);

        return beans.entrySet().stream()
                .filter(entry -> getAnnotatedComponentType(entry.getKey()).equals(componentType))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElseThrow(() -> new NoSuchBeanDefinitionException(serviceClass.getSimpleName(), componentType));

    }

    /**
     * Extracts the service name from a bean's {@link ExcalibaseService} annotation.
     *
     * <p>This helper method inspects the specified bean for the {@link ExcalibaseService}
     * annotation and returns its {@code serviceName} value. If the annotation is not
     * present, an empty string is returned.</p>
     *
     * @param beanName the name of the bean to inspect
     * @return the service name from the annotation, or empty string if not annotated
     */
    private String getAnnotatedComponentType(String beanName) {
        ExcalibaseService excalibaseService = applicationContext.findAnnotationOnBean(beanName, ExcalibaseService.class);
        return excalibaseService == null ? "" : excalibaseService.serviceName();
    }
}