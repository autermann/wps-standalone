/*
 * Copyright (C) 2013-2015 Christian Autermann
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package com.github.autermann.wps.commons;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.n52.wps.server.database.DatabaseFactory.PROPERTY_NAME_DATABASE_CLASS_NAME;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import net.opengis.ows.x11.HTTPDocument.HTTP;
import net.opengis.ows.x11.OperationDocument.Operation;
import net.opengis.ows.x11.OperationsMetadataDocument.OperationsMetadata;
import net.opengis.wps.x100.CapabilitiesDocument;
import net.opengis.wps.x100.LanguagesDocument.Languages;
import net.opengis.wps.x100.WPSCapabilitiesType;

import org.apache.xmlbeans.XmlException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

import org.n52.wps.DatabaseDocument.Database;
import org.n52.wps.DatahandlersDocument.Datahandlers;
import org.n52.wps.FormatDocument;
import org.n52.wps.GeneratorDocument.Generator;
import org.n52.wps.ParserDocument.Parser;
import org.n52.wps.PropertyDocument.Property;
import org.n52.wps.RepositoryDocument.Repository;
import org.n52.wps.ServerDocument;
import org.n52.wps.WPSConfigurationDocument;
import org.n52.wps.WPSConfigurationDocument.WPSConfiguration;
import org.n52.wps.commons.WPSConfig;
import org.n52.wps.io.IGenerator;
import org.n52.wps.io.IParser;
import org.n52.wps.server.CapabilitiesConfiguration;
import org.n52.wps.server.IAlgorithm;
import org.n52.wps.server.IAlgorithmRepository;
import org.n52.wps.server.LocalAlgorithmRepository;
import org.n52.wps.server.RetrieveResultServlet;
import org.n52.wps.server.WebProcessingService;

public class WPS {
    private static final String UPDATE_SEQUENCE = "1";
    private static final String SERVICE_VERSION = "1.0.0";
    private static final String EXECUTE = "Execute";
    private static final String DESCRIBE_PROCESS = "DescribeProcess";
    private static final String GET_CAPABILITIES = "GetCapabilities";
    private static final String SERVICE = "WPS";
    private static final String LANGUAGE = "en-US";
    private static final String EMPTY = "";
    public static final String WEB_PROCESSING_SERVICE_PATH = "/WebProcessingService";
    public static final String RETRIEVE_RESULT_SERVLET_PATH = "/RetrieveResultServlet";
    public static final String ROOT_CONTEXT = "/";
    private static final int MAX_QUEUED_TASKS = 100;
    private static final int COMPUTATION_TIME_OUT = 5;
    private static final int MAX_POOL_SIZE = 20;
    private static final int MIN_POOL_SIZE = 10;
    private static final int KEEP_ALIVE_SECONDS = 1000;

    private final AtomicInteger parserCount = new AtomicInteger(0);
    private final AtomicInteger generatorCount = new AtomicInteger(0);
    private final Server server;
    private final ReentrantLock lock = new ReentrantLock();
    private final WPSConfigurationDocument config;

    public WPS(String host, int port) throws Exception{
        this(host, port, false);
    }

    public WPS(String host, int port, boolean https) throws Exception{
        checkArgument(port > 0 && host != null && !host.isEmpty());
        this.config = createEmptyConfig(host, port, https);
        this.server = createServer(port);
    }

    protected Server createServer(int port) throws Exception {
        Server s = new Server(port);
        ServletContextHandler sch = new ServletContextHandler(s, ROOT_CONTEXT);
        sch.addServlet(WebProcessingService.class, WEB_PROCESSING_SERVICE_PATH);
        sch.addServlet(RetrieveResultServlet.class, RETRIEVE_RESULT_SERVLET_PATH);
        return s;
    }


    private WPSConfigurationDocument createEmptyConfig(String host, int port, boolean https) {
        WPSConfigurationDocument document = WPSConfigurationDocument.Factory
                .newInstance();
        WPSConfiguration wpsConfig = document.addNewWPSConfiguration();
        wpsConfig.addNewAlgorithmRepositoryList();
        wpsConfig.addNewRemoteRepositoryList();
        Datahandlers datahandlers = wpsConfig.addNewDatahandlers();
        datahandlers.addNewGeneratorList();
        datahandlers.addNewParserList();
        ServerDocument.Server serverConfig = wpsConfig.addNewServer();
        serverConfig.setHostport(String.valueOf(port));
        serverConfig.setHostname(host);
        serverConfig.setProtocol(https ? "https" : "http");
        serverConfig.setWebappPath(EMPTY);
        serverConfig.setCacheCapabilites(true);
        serverConfig.setIncludeDataInputsInResponse(false);
        serverConfig.setMinPoolSize(BigInteger.valueOf(MIN_POOL_SIZE));
        serverConfig.setMaxPoolSize(BigInteger.valueOf(MAX_POOL_SIZE));
        serverConfig.setKeepAliveSeconds(BigInteger.valueOf(KEEP_ALIVE_SECONDS));
        serverConfig.setComputationTimeoutMilliSeconds(String.valueOf(COMPUTATION_TIME_OUT));
        serverConfig.setMaxQueuedTasks(BigInteger.valueOf(MAX_QUEUED_TASKS));
        createDatabaseConfig(serverConfig.addNewDatabase());
        return document;
    }

    public WPS setMinPoolSize(int size) {
        getConfig().getServer().setMinPoolSize(BigInteger.valueOf(size));
        return this;
    }

    public WPS setMaxPoolSize(int size) {
        getConfig().getServer().setMaxPoolSize(BigInteger.valueOf(size));
        return this;
    }

    public WPS setIncludeDataInputsInResponse(boolean include) {
        getConfig().getServer().setIncludeDataInputsInResponse(include);
        return this;
    }

    public WPS setKeepAlive(int seconds) {
        getConfig().getServer().setKeepAliveSeconds(BigInteger.valueOf(seconds));
        return this;
    }

    public WPS setComputationTimeout(int milliseconds) {
        getConfig().getServer().setComputationTimeoutMilliSeconds(String
                .valueOf(milliseconds));
        return this;
    }

    public WPS setMaxQueuedTasks(int max) {
        getConfig().getServer().setMaxQueuedTasks(BigInteger.valueOf(max));
        return this;
    }

    protected void createDatabaseConfig(Database database) {
        Property databaseClassName = database.addNewProperty();
        databaseClassName.setName(PROPERTY_NAME_DATABASE_CLASS_NAME);
        databaseClassName.setStringValue("org.n52.wps.server.database.FlatFileDatabase");
    }

    private CapabilitiesDocument createCapabilitiesSkeleton() {
        CapabilitiesDocument doc = CapabilitiesDocument.Factory.newInstance();
        WPSCapabilitiesType caps = doc.addNewCapabilities();
        caps.setLang(LANGUAGE);
        Languages languages = caps.addNewLanguages();
        languages.addNewDefault().setLanguage(LANGUAGE);
        languages.addNewSupported().addLanguage(LANGUAGE);
        caps.addNewService().setStringValue(SERVICE);
        caps.setVersion(SERVICE_VERSION);
        caps.setUpdateSequence(UPDATE_SEQUENCE);
        OperationsMetadata operationsMetadata = caps.addNewOperationsMetadata();
        Operation getCapabilities = operationsMetadata.addNewOperation();
        getCapabilities.setName(GET_CAPABILITIES);
        getCapabilities.addNewDCP().addNewHTTP().addNewGet().setHref(EMPTY);
        Operation describeProcess = operationsMetadata.addNewOperation();
        describeProcess.setName(DESCRIBE_PROCESS);
        describeProcess.addNewDCP().addNewHTTP().addNewGet().setHref(EMPTY);
        Operation execute = operationsMetadata.addNewOperation();
        execute.setName(EXECUTE);
        HTTP executeHttp = execute.addNewDCP().addNewHTTP();
        executeHttp.addNewGet().setHref(EMPTY);
        executeHttp.addNewPost().setHref(EMPTY);
        return doc;
    }

    private void configure() throws XmlException, IOException {
        WPSConfig.forceInitialization(getConfigDocument().newInputStream());
        CapabilitiesConfiguration.getInstance(createCapabilitiesSkeleton());
    }

    private WPSConfiguration getConfig() {
        return getConfigDocument().getWPSConfiguration();
    }

    private WPSConfigurationDocument getConfigDocument() {
        return config;
    }

    public WPS addAlgorithmRepository(
            Class<? extends IAlgorithmRepository> repoClass) {
        return addAlgorithmRepository(repoClass, null);
    }

    public WPS addAlgorithmRepository(
            Class<? extends IAlgorithmRepository> repoClass,
            Map<String, ? extends Collection<String>> properties) {
        lock.lock();
        try {
            checkState(!isRunning());
            _addAlgorithmRepository(repoClass, properties);
            return this;
        } finally {
            lock.unlock();
        }
    }

    private Repository _addAlgorithmRepository(
            Class<? extends IAlgorithmRepository> repoClass,
            Map<String, ? extends Collection<String>> properties) {
        Repository repo = Arrays.stream(getConfig()
                                    .getAlgorithmRepositoryList()
                                    .getRepositoryArray())
                .filter(r -> r.getClassName().equals(repoClass.getName()))
                .findAny()
                .orElseGet(getConfig().getAlgorithmRepositoryList()::addNewRepository);
        repo.setActive(true);
        repo.setClassName(repoClass.getName());
        repo.setName(repoClass.getName());
        if (properties != null) {
            properties.keySet().stream()
                    .forEach((property) -> {
                        properties.get(property).stream().forEach((value) -> {
                                    Property p = repo.addNewProperty();
                                    p.setActive(true);
                                    p.setName(property);
                                    p.setStringValue(value);
                                });
            });
        }
        return repo;
    }

    public WPS addAlgorithm(Class<? extends IAlgorithm> algoClass) {
        lock.lock();
        try {
            checkState(!isRunning());
            Repository repo
                    = _addAlgorithmRepository(LocalAlgorithmRepository.class, null);
            Property p = repo.addNewProperty();
            p.setName("Algorithm");
            p.setActive(true);
            p.setStringValue(algoClass.getName());
            return this;
        } finally {
            lock.unlock();
        }
    }

    public WPS addParser(Class<? extends IParser> clazz) {
        return addParser(clazz, null, null);

    }

    public WPS addParser(Class<? extends IParser> clazz,
                         Iterable<Format> formats) {
        return addParser(clazz, formats, null);

    }

    public WPS addParser(Class<? extends IParser> clazz,
                         Iterable<Format> formats,
                         Map<String, ? extends Collection<String>> properties) {
        lock.lock();
        try {
            checkState(!isRunning());
            Parser parser = getConfig().getDatahandlers()
                    .getParserList().addNewParser();
            parser.setActive(true);
            parser.setClassName(clazz.getName());
            parser.setName("parser" + parserCount.getAndIncrement());
            if (formats != null) {
                for (Format f : formats) {
                    FormatDocument.Format xb = parser.addNewFormat();
                    f.setTo(xb::setEncoding, xb::setMimetype, xb::setSchema);
                }
            }
             if (properties != null) {
                properties.forEach((key, values) -> {
                    values.forEach(value -> {
                        Property p = parser.addNewProperty();
                        p.setActive(true);
                        p.setName(key);
                        p.setStringValue(value);
                    });
                });
            }
            return this;
        } finally {
            lock.unlock();
        }
    }

    public WPS addGenerator(Class<? extends IGenerator> clazz) {
        return addGenerator(clazz, null, null);
    }

    public WPS addGenerator(Class<? extends IGenerator> clazz,
                            Iterable<Format> formats) {
        return addGenerator(clazz, formats, null);
    }

    public WPS addGenerator(Class<? extends IGenerator> clazz,
                            Iterable<Format> formats,
                            Map<String, ? extends Collection<String>> properties) {
        lock.lock();
        try {
            checkState(!isRunning());
            Generator generator = getConfig().getDatahandlers()
                    .getGeneratorList().addNewGenerator();
            generator.setActive(true);
            generator.setClassName(clazz.getName());
            generator.setName("generator" + generatorCount.getAndIncrement());
            if (formats != null) {
                for (Format f : formats) {
                    FormatDocument.Format xb = generator.addNewFormat();
                    f.setTo(xb::setEncoding, xb::setMimetype, xb::setSchema);
                }
            }
            if (properties != null) {
                properties.forEach((key, values) -> {
                    values.forEach(value -> {
                        Property p = generator.addNewProperty();
                        p.setActive(true);
                        p.setName(key);
                        p.setStringValue(value);
                    });
                });
            }
            return this;
        } finally {
            lock.unlock();
        }
    }

    private Server getServer() {
        return server.getServer();
    }

    public void start() throws Exception {
        lock.lock();
        try {
            checkState(!isRunning());
            configure();
            getServer().start();
        } finally {
            lock.unlock();
        }

    }

    public void stop() throws Exception {
        lock.lock();
        try {
            checkState(isRunning());
            getServer().stop();
        } finally {
            lock.unlock();
        }
    }

    public boolean isRunning() {
        lock.lock();
        try {
            return getServer().isRunning();
        } finally {
            lock.unlock();
        }
    }

    public boolean isStarted() {
        lock.lock();
        try {
            return getServer().isStarted();
        } finally {
            lock.unlock();
        }
    }

    public boolean isStarting() {
        lock.lock();
        try {
            return getServer().isStarting();
        } finally {
            lock.unlock();
        }
    }

    public boolean isStopping() {
        lock.lock();
        try {
            return getServer().isStopping();
        } finally {
            lock.unlock();
        }
    }

    public boolean isStopped() {
        lock.lock();
        try {
            return getServer().isStopped();
        } finally {
            lock.unlock();
        }
    }

    public boolean isFailed() {
        lock.lock();
        try {
            return server.isFailed();
        } finally {
            lock.unlock();
        }
    }
}
