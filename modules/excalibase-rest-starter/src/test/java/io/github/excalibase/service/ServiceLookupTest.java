package io.github.excalibase.service;

import io.github.excalibase.annotation.ExcalibaseService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ServiceLookupTest {

    @Mock
    private ApplicationContext applicationContext;

    private ServiceLookup serviceLookup;

    // Minimal test interface
    interface TestService {
        String hello();
    }

    // Implementation annotated with ExcalibaseService
    @ExcalibaseService(serviceName = "postgres")
    static class PostgresTestService implements TestService {
        @Override
        public String hello() { return "postgres"; }
    }

    @ExcalibaseService(serviceName = "mysql")
    static class MysqlTestService implements TestService {
        @Override
        public String hello() { return "mysql"; }
    }

    @BeforeEach
    void setUp() {
        serviceLookup = new ServiceLookup(applicationContext);
    }

    @Test
    void forBean_findsMatchingBean_byAnnotatedServiceName() {
        PostgresTestService postgresImpl = new PostgresTestService();
        MysqlTestService mysqlImpl = new MysqlTestService();

        when(applicationContext.getBeansOfType(TestService.class))
            .thenReturn(Map.of("postgresTestService", postgresImpl, "mysqlTestService", mysqlImpl));

        when(applicationContext.findAnnotationOnBean(eq("postgresTestService"), eq(ExcalibaseService.class)))
            .thenReturn(PostgresTestService.class.getAnnotation(ExcalibaseService.class));

        when(applicationContext.findAnnotationOnBean(eq("mysqlTestService"), eq(ExcalibaseService.class)))
            .thenReturn(MysqlTestService.class.getAnnotation(ExcalibaseService.class));

        TestService result = serviceLookup.forBean(TestService.class, "postgres");
        assertEquals("postgres", result.hello());
    }

    @Test
    void forBean_findsSecondMatchingBean_byAnnotatedServiceName() {
        PostgresTestService postgresImpl = new PostgresTestService();
        MysqlTestService mysqlImpl = new MysqlTestService();

        when(applicationContext.getBeansOfType(TestService.class))
            .thenReturn(Map.of("postgresTestService", postgresImpl, "mysqlTestService", mysqlImpl));

        when(applicationContext.findAnnotationOnBean(eq("postgresTestService"), eq(ExcalibaseService.class)))
            .thenReturn(PostgresTestService.class.getAnnotation(ExcalibaseService.class));

        when(applicationContext.findAnnotationOnBean(eq("mysqlTestService"), eq(ExcalibaseService.class)))
            .thenReturn(MysqlTestService.class.getAnnotation(ExcalibaseService.class));

        TestService result = serviceLookup.forBean(TestService.class, "mysql");
        assertEquals("mysql", result.hello());
    }

    @Test
    void forBean_throwsNoSuchBeanDefinitionException_whenNoBeanFound() {
        PostgresTestService postgresImpl = new PostgresTestService();

        when(applicationContext.getBeansOfType(TestService.class))
            .thenReturn(Map.of("postgresTestService", postgresImpl));

        when(applicationContext.findAnnotationOnBean(eq("postgresTestService"), eq(ExcalibaseService.class)))
            .thenReturn(PostgresTestService.class.getAnnotation(ExcalibaseService.class));

        assertThrows(NoSuchBeanDefinitionException.class,
            () -> serviceLookup.forBean(TestService.class, "oracle"));
    }

    @Test
    void forBean_throwsNoSuchBeanDefinitionException_whenNoBeanRegistered() {
        when(applicationContext.getBeansOfType(TestService.class))
            .thenReturn(Map.of());

        assertThrows(NoSuchBeanDefinitionException.class,
            () -> serviceLookup.forBean(TestService.class, "postgres"));
    }

    @Test
    void forBean_beanWithoutAnnotation_notMatchedForAnyServiceName() {
        // Bean without ExcalibaseService annotation -> getAnnotatedComponentType returns ""
        PostgresTestService postgresImpl = new PostgresTestService();

        when(applicationContext.getBeansOfType(TestService.class))
            .thenReturn(Map.of("unannotatedBean", postgresImpl));

        when(applicationContext.findAnnotationOnBean(eq("unannotatedBean"), eq(ExcalibaseService.class)))
            .thenReturn(null); // no annotation

        assertThrows(NoSuchBeanDefinitionException.class,
            () -> serviceLookup.forBean(TestService.class, "postgres"));
    }
}
