/*
 *  Copyright (C) 2007 - 2011 GeoSolutions S.A.S.
 *  http://www.geo-solutions.it
 *
 *  GPLv3 + Classpath exception
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.geosolutions.geostore.core.dao;

import it.geosolutions.geostore.core.model.Attribute;
import it.geosolutions.geostore.core.model.Category;
import it.geosolutions.geostore.core.model.Resource;
import it.geosolutions.geostore.core.model.StoredData;
import it.geosolutions.geostore.core.model.User;
import it.geosolutions.geostore.core.model.UserAttribute;
import it.geosolutions.geostore.core.model.UserGroup;
import java.util.Date;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import junit.framework.TestCase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Class BaseDAOTest
 *
 * @author ETj (etj at geo-solutions.it)
 * @author Tobia di Pisa (tobia.dipisa at geo-solutions.it)
 */
public abstract class BaseDAOTest extends TestCase {

    protected final Logger LOGGER;

    protected static StoredDataDAO storedDataDAO;

    protected static ResourceDAO resourceDAO;

    protected static AttributeDAO attributeDAO;

    protected static CategoryDAO categoryDAO;

    protected static SecurityDAO securityDAO;

    protected static SecurityDAO externalSecurityDAO;

    protected static UserAttributeDAO userAttributeDAO;

    protected static UserDAO userDAO;

    protected static UserGroupDAO userGroupDAO;

    protected static ClassPathXmlApplicationContext ctx = null;

    protected static EntityManagerFactory emf = null;

    public BaseDAOTest() {
        LOGGER = LogManager.getLogger(getClass());

        synchronized (BaseDAOTest.class) {
            if (ctx == null) {
                String[] paths = {
                    "applicationContext.xml", "applicationContext-geostoreDatasource.xml"
                };
                ctx = new ClassPathXmlApplicationContext(paths);

                storedDataDAO = (StoredDataDAO) ctx.getBean("storedDataDAO");
                resourceDAO = (ResourceDAO) ctx.getBean("resourceDAO");
                attributeDAO = (AttributeDAO) ctx.getBean("attributeDAO");
                categoryDAO = (CategoryDAO) ctx.getBean("categoryDAO");
                securityDAO = (SecurityDAO) ctx.getBean("securityDAO");
                externalSecurityDAO = (SecurityDAO) ctx.getBean("externalSecurityDAO");
                userAttributeDAO = (UserAttributeDAO) ctx.getBean("userAttributeDAO");
                userDAO = (UserDAO) ctx.getBean("userDAO");
                userGroupDAO = (UserGroupDAO) ctx.getBean("userGroupDAO");

                // Force initialization of EntityManagerFactory and schema creation
                try {
                    emf = (EntityManagerFactory) ctx.getBean("geostoreEntityManagerFactory");
                    EntityManager em = emf.createEntityManager();
                    
                    // Start transaction
                    em.getTransaction().begin();
                    
                    try {
                        // Create database and schema if they don't exist
                        em.createNativeQuery("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'GeoStore') CREATE DATABASE GeoStore").executeUpdate();
                        em.createNativeQuery("USE GeoStore").executeUpdate();
                        em.createNativeQuery("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbo') EXEC('CREATE SCHEMA dbo')").executeUpdate();
                        
                        // Drop existing tables if they exist
                        String[] dropTableQueries = {
                            "IF OBJECT_ID('gs_usergroup_members', 'U') IS NOT NULL DROP TABLE gs_usergroup_members",
                            "IF OBJECT_ID('gs_security', 'U') IS NOT NULL DROP TABLE gs_security",
                            "IF OBJECT_ID('gs_stored_data', 'U') IS NOT NULL DROP TABLE gs_stored_data",
                            "IF OBJECT_ID('gs_attribute', 'U') IS NOT NULL DROP TABLE gs_attribute",
                            "IF OBJECT_ID('gs_resource', 'U') IS NOT NULL DROP TABLE gs_resource",
                            "IF OBJECT_ID('gs_category', 'U') IS NOT NULL DROP TABLE gs_category",
                            "IF OBJECT_ID('gs_user_attribute', 'U') IS NOT NULL DROP TABLE gs_user_attribute",
                            "IF OBJECT_ID('gs_user', 'U') IS NOT NULL DROP TABLE gs_user",
                            "IF OBJECT_ID('gs_usergroup', 'U') IS NOT NULL DROP TABLE gs_usergroup"
                        };
                        
                        for (String query : dropTableQueries) {
                            em.createNativeQuery(query).executeUpdate();
                        }
                        
                        // Create tables explicitly
                        String[] createTableQueries = {
                            "CREATE TABLE gs_category (id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL)",
                            "CREATE TABLE gs_resource (id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, creation DATETIME NOT NULL, description VARCHAR(10000) NULL, lastUpdate DATETIME NULL, metadata VARCHAR(30000) NULL, name VARCHAR(255) NOT NULL, category_id BIGINT NOT NULL, creator VARCHAR(255) NULL, editor VARCHAR(255) NULL, advertised BIT DEFAULT 1, CONSTRAINT FK_RESOURCE_CATEGORY FOREIGN KEY (category_id) REFERENCES gs_category(id))",
                            "CREATE TABLE gs_stored_data (id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, data TEXT NULL, resource_id BIGINT NOT NULL, CONSTRAINT FK_DATA_RESOURCE FOREIGN KEY (resource_id) REFERENCES gs_resource(id))",
                            "CREATE TABLE gs_attribute (id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, type VARCHAR(255) NOT NULL, value VARCHAR(255) NULL, resource_id BIGINT NOT NULL, CONSTRAINT FK_ATTRIBUTE_RESOURCE FOREIGN KEY (resource_id) REFERENCES gs_resource(id))",
                            "CREATE TABLE gs_user (id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, password VARCHAR(255) NULL, role VARCHAR(255) NOT NULL)",
                            "CREATE TABLE gs_user_attribute (id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, type VARCHAR(255) NOT NULL, value VARCHAR(255) NULL, user_id BIGINT NOT NULL, CONSTRAINT FK_USER_ATTRIBUTE FOREIGN KEY (user_id) REFERENCES gs_user(id))",
                            "CREATE TABLE gs_usergroup (id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, groupName VARCHAR(255) NOT NULL)",
                            "CREATE TABLE gs_security (id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, canRead BIT NOT NULL, canWrite BIT NOT NULL, resource_id BIGINT NOT NULL, user_id BIGINT NULL, group_id BIGINT NULL, CONSTRAINT FK_SECURITY_RESOURCE FOREIGN KEY (resource_id) REFERENCES gs_resource(id), CONSTRAINT FK_SECURITY_USER FOREIGN KEY (user_id) REFERENCES gs_user(id), CONSTRAINT FK_SECURITY_GROUP FOREIGN KEY (group_id) REFERENCES gs_usergroup(id))",
                            "CREATE TABLE gs_usergroup_members (user_id BIGINT NOT NULL, group_id BIGINT NOT NULL, PRIMARY KEY (user_id, group_id), CONSTRAINT FK_MEMBER_USER FOREIGN KEY (user_id) REFERENCES gs_user(id), CONSTRAINT FK_MEMBER_GROUP FOREIGN KEY (group_id) REFERENCES gs_usergroup(id))"
                        };
                        
                        for (String query : createTableQueries) {
                            try {
                                em.createNativeQuery(query).executeUpdate();
                            } catch (Exception e) {
                                LOGGER.error("Failed to execute query: " + query, e);
                                throw e;
                            }
                        }
                        
                        // Force schema creation by persisting test entities
                        Category category = new Category();
                        category.setName("test_category");
                        em.persist(category);
                        
                        Resource resource = new Resource();
                        resource.setName("test_resource");
                        resource.setCreation(new Date());
                        resource.setCategory(category);
                        em.persist(resource);
                        
                        // Flush to force table creation
                        em.flush();
                        
                        // Clean up test data
                        em.remove(resource);
                        em.remove(category);
                        
                        // Commit transaction
                        em.getTransaction().commit();
                    } catch (Exception e) {
                        LOGGER.error("Failed to create database schema", e);
                        if (em.getTransaction().isActive()) {
                            em.getTransaction().rollback();
                        }
                        throw e;
                    } finally {
                        em.close();
                    }
                    
                    LOGGER.info("Successfully initialized database schema");
                } catch (Exception e) {
                    LOGGER.error("Critical error during database initialization", e);
                    throw new RuntimeException("Failed to initialize database", e);
                }
            }
        }
    }

    @Override
    protected void setUp() throws Exception {
        LOGGER.info("################ Running " + getClass().getSimpleName() + "::" + getName());
        super.setUp();
        removeAll();
        LOGGER.info("##### Ending setup for " + getName() + " ###----------------------");
    }

    @Test
    public void testCheckDAOs() {
        assertNotNull(storedDataDAO);
        assertNotNull(resourceDAO);
        assertNotNull(attributeDAO);
        assertNotNull(categoryDAO);
        assertNotNull(securityDAO);
        assertNotNull(externalSecurityDAO);
        assertNotNull(userAttributeDAO);
        assertNotNull(userDAO);
        assertNotNull(userGroupDAO);
    }

    protected void removeAll() {
        removeAllResource();
        removeAllStoredData();
        removeAllAttribute();
        removeAllUserAttribute();
        removeAllCategory();
        removeAllUser();
        removeAllUserGroup();
    }

    private void removeAllUser() {
        List<User> list = userDAO.findAll();
        for (User item : list) {
            LOGGER.info("Removing " + item.getId());
            boolean ret = userDAO.remove(item);
            assertTrue("User not removed", ret);
        }

        assertEquals("Users have not been properly deleted", 0, userDAO.count(null));
    }

    private void removeAllUserGroup() {
        List<UserGroup> list = userGroupDAO.findAll();
        for (UserGroup item : list) {
            LOGGER.info("Removing " + item.getId());
            boolean ret = userGroupDAO.remove(item);
            assertTrue("UserGroup not removed", ret);
        }

        assertEquals("UserGroup have not been properly deleted", 0, userGroupDAO.count(null));
    }

    private void removeAllCategory() {
        List<Category> list = categoryDAO.findAll();
        for (Category item : list) {
            LOGGER.info("Removing " + item.getId());
            boolean ret = categoryDAO.remove(item);
            assertTrue("Category not removed", ret);
        }

        assertEquals("Category have not been properly deleted", 0, categoryDAO.count(null));
    }

    private void removeAllUserAttribute() {
        List<UserAttribute> list = userAttributeDAO.findAll();
        for (UserAttribute item : list) {
            LOGGER.info("Removing " + item.getId());
            boolean ret = userAttributeDAO.remove(item);
            assertTrue("UserAttribute not removed", ret);
        }

        assertEquals(
                "UserAttribute have not been properly deleted", 0, userAttributeDAO.count(null));
    }

    protected void removeAllStoredData() {
        List<StoredData> list = storedDataDAO.findAll();
        for (StoredData item : list) {
            LOGGER.info("Removing " + item.getId());
            boolean ret = storedDataDAO.remove(item);
            assertTrue("StoredData not removed", ret);
        }

        assertEquals("StoredData have not been properly deleted", 0, storedDataDAO.count(null));
    }

    private void removeAllResource() {
        List<Resource> list = resourceDAO.findAll();
        for (Resource item : list) {
            LOGGER.info("Removing " + item.getId());
            boolean ret = resourceDAO.remove(item);
            assertTrue("Resource not removed", ret);
        }

        assertEquals("Resource have not been properly deleted", 0, resourceDAO.count(null));
    }

    private void removeAllAttribute() {
        List<Attribute> list = attributeDAO.findAll();
        for (Attribute item : list) {
            LOGGER.info("Removing " + item.getId());
            boolean ret = attributeDAO.remove(item);
            assertTrue("DataType not removed", ret);
        }

        assertEquals("DataType have not been properly deleted", 0, attributeDAO.count(null));
    }
}
