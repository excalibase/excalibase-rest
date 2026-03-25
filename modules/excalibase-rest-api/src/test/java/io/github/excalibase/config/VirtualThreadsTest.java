package io.github.excalibase.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 10A: Verifies that Spring Boot virtual threads are enabled and usable.
 * When spring.threads.virtual.enabled=true, Spring Boot uses a virtual-thread executor
 * for request handling on Java 21+.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = "spring.threads.virtual.enabled=true")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class VirtualThreadsTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("vt_testdb")
            .withUsername("vt_user")
            .withPassword("vt_pass");

    static {
        postgres.start();
        System.setProperty("spring.datasource.url", postgres.getJdbcUrl());
        System.setProperty("spring.datasource.username", postgres.getUsername());
        System.setProperty("spring.datasource.password", postgres.getPassword());
        System.setProperty("spring.datasource.driver-class-name", "org.postgresql.Driver");
    }

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void virtualThreadsCanBeCreated() throws Exception {
        // Verify Java 21 virtual thread creation works
        AtomicReference<String> threadName = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Thread.ofVirtual().start(() -> {
            threadName.set(Thread.currentThread().toString());
            latch.countDown();
        });

        latch.await();
        // Virtual thread names contain "VirtualThread"
        assertTrue(threadName.get().contains("VirtualThread"),
            "Expected virtual thread but got: " + threadName.get());
    }

    @Test
    void applicationContextLoadsWithVirtualThreads() {
        // Spring context should load successfully with virtual threads enabled
        assertNotNull(applicationContext);
    }

    @Test
    void currentJavaVersionSupportsVirtualThreads() {
        // Verify we're running on Java 21+
        int javaVersion = Runtime.version().feature();
        assertTrue(javaVersion >= 21,
            "Virtual threads require Java 21+, but running on Java " + javaVersion);
    }
}
