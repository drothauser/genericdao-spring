/*
 * Copyright (c) 2009 Rothsmith LLC, All rights reserved.
 */
package net.rothsmith.dao.spring;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import net.rothsmith.dao.JdbcDao;

/**
 * {@link NamedParameterJdbcTemplate} implementation of the {@link JdbcDao}
 * interface.
 * 
 * @author drothauser
 * 
 * @param <T>
 *            The class of the DTO being persisted or null if no DTO used. The
 *            way this class is used depends on the type of DML operation:
 *            <ul>
 *            <li>SELECT - defines the return type
 *            <ul>
 *            <li>Note that if the <code>type</code> field is null, a
 *            {@link Map} class will be used.
 *            </ul>
 *            <li>INSERT, UPDATE, DELETE - defines the parameter type.
 *            </ul>
 * 
 * @param <P>
 *            Parameter type
 */
@Repository
@SuppressWarnings("PMD.TooManyMethods")
public class NamedParamJdbcTemplateDao<T, P> implements JdbcDao<T, P> {

	/**
	 * The class of the DTO being persisted or null of no DTO used. The way this
	 * class is used depends on the type of DML operation:
	 * <ul>
	 * <li>SELECT - defines the return type
	 * <ul>
	 * <li>Note that if the <code>type</code> field is null, a {@link Map} class
	 * will be used.
	 * </ul>
	 * <li>INSERT, UPDATE, DELETE - defines the parameter type.
	 * </ul>
	 */
	private Class<T> type;

	/**
	 * JDBC {@link DataSource}.
	 */
	private DataSource dataSource;

	/**
	 * The Spring {@link NamedParameterJdbcTemplate} object used to execute
	 * statements.
	 */
	private NamedParameterJdbcTemplate jdbcTemplate;

	/**
	 * SQL statement {@link Map}.
	 */
	private Map<String, String> statementMap;

	/**
	 * Construct DAO with DTO type.
	 * 
	 * @param type
	 *            Type of DTO
	 */
	public NamedParamJdbcTemplateDao(final Class<T> type) {
		this.type = type;
	}

	/**
	 * Default Constructor.
	 */
	public NamedParamJdbcTemplateDao() {
		// for constructing DAO with no DTO type.
	}

	/**
	 * Return DTO type.
	 * 
	 * @return DTO type
	 */
	@Override
	public final Class<T> getType() {
		return type;
	}

	/**
	 * Set DTO type. This is typically set initially in the constructor.
	 * 
	 * @param type
	 *            DTO type
	 */
	@Override
	public final void setType(final Class<T> type) {
		this.type = type;
	}

	/**
	 * Return DataSource.
	 * 
	 * @return DataSource
	 */
	@Override
	public final DataSource getDataSource() {
		return dataSource;
	}

	/**
	 * Set DataSource and create the jdbcTemplate instance variable.
	 * 
	 * @param dataSource
	 *            DataSource
	 */
	@Override
	public final void setDataSource(final DataSource dataSource) {
		this.dataSource = dataSource;
		jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}

	/**
	 * Get the {@link NamedParameterJdbcTemplate} instance.
	 * 
	 * @return the JdbcTemplate
	 */
	public final NamedParameterJdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	/**
	 * Set the {@link NamedParameterJdbcTemplate} instance.
	 * 
	 * @param jdbcTemplate
	 *            a {@link NamedParameterJdbcTemplate} instance
	 */
	public final void setJdbcTemplate(
	    final NamedParameterJdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Get statement map.
	 * 
	 * @return the Map
	 */
	@Override
	public final Map<String, String> getStatementMap() {
		return statementMap;
	}

	/**
	 * Set statement map.
	 * 
	 * @param statementMap
	 *            the Map to set
	 */
	@Override
	public final void setStatementMap(final Map<String, String> statementMap) {
		this.statementMap = statementMap;
	}

	/**
	 * Run a SQL SELECT statement given the specified parameters.
	 * 
	 * @param sql
	 *            The SQL statement to execute.
	 * @param params
	 *            The parameters used by the SQL statement
	 * @return a {@link List} of type T objects.
	 */
	@SuppressWarnings({ "unchecked", "PMD.AvoidDuplicateLiterals" })
	private List<T> selectDtoParams(final String sql, final P params) {

		List<T> list = null;

		if (type.isInstance(params)) {
			list = jdbcTemplate.query(sql,
			    new BeanPropertySqlParameterSource(params),
			    (RowMapper<T>) BeanPropertyRowMapper.newInstance(type));
		} else if (params instanceof Map) {
			list = jdbcTemplate.query(sql, (Map<String, ?>) params,
			    (RowMapper<T>) BeanPropertyRowMapper.newInstance(type));
		} else if (params instanceof Object[]) {
			list = jdbcTemplate.getJdbcOperations().query(sql,
			    (RowMapper<T>) BeanPropertyRowMapper.newInstance(type), params);
		} else {
			throw new IllegalArgumentException("Parameters must be of type "
			    + type.getCanonicalName() + ", " + Map.class.getCanonicalName()
			    + " or " + Object[].class.getCanonicalName()
			    + " type passed was " + params.getClass().getCanonicalName());
		}
		return list;
	}

	/**
	 * 
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            The parameters used by the SQL statement
	 * @return a {@link List} of type T objects.
	 */
	@SuppressWarnings("unchecked")
	private List<T> selectNoDtoParams(final String sql, final P params) {
		Object list = null;
		if (params instanceof Map) {
			list =
			    jdbcTemplate.queryForList(sql, (Map<String, ?>) params, type);
		} else if (params instanceof Object[]) {
			list = jdbcTemplate.getJdbcOperations().queryForList(sql, params);
		} else {
			throw new IllegalArgumentException("Parameters must be of type "
			    + Map.class.getCanonicalName() + " or "
			    + Object[].class.getCanonicalName() + " type passed was "
			    + params.getClass().getCanonicalName());
		}
		return (List<T>) list;
	}

	/**
	 * Return a list of DTOs using testParams to populate criteria.
	 * 
	 * @param statementId
	 *            The key of the {@link NamedParamJdbcTemplateDao#statementMap}
	 *            that points to the SQL statement to execute.
	 * @param params
	 *            the parameter list
	 * 
	 * @return List of DTOs
	 */
	@SuppressWarnings("unchecked")
	public final List<T> selectByStatement(String statementId, P params) {

		final String sql = statementMap.get(statementId);
		if (StringUtils.isEmpty(sql)) {
			throw new IllegalArgumentException(String.format(
			    "No sql statement found for statement \"%s\"", statementId));
		}

		Object list = null;

		if (type == null) {
			// Query with no DTO and no parameters
			if (params == null) {
				list = jdbcTemplate.getJdbcOperations().queryForList(sql);
			} else {
				list = selectNoDtoParams(sql, params);
			}
		} else {
			if (params == null) {
				list = jdbcTemplate.query(sql,
				    (RowMapper<T>) BeanPropertyRowMapper.newInstance(type));
				// Query with DTO and parameters
			} else {
				list = selectDtoParams(sql, params);
			}
		}
		return (List<T>) list;
	}

	/**
	 * This method executes an ad hoc SQL statement with the specified
	 * parameters and returns a list of DTOs.
	 * 
	 * @param sql
	 *            ad hoc SQL statement
	 * @param params
	 *            the parameter list
	 * 
	 * @return List of DTOs
	 */
	@SuppressWarnings("unchecked")
	public final List<T> select(final String sql, final P params) {
		Object list = null;
		if (type == null) {
			if (params == null) {
				list = jdbcTemplate.getJdbcOperations().queryForList(sql);
				// Query no DTO and parameters
			} else {
				list = selectNoDtoParams(sql, params);
			}
		} else {
			// Query with DTO and no parameters
			if (params == null) {
				list = jdbcTemplate.query(sql,
				    (RowMapper<T>) BeanPropertyRowMapper.newInstance(type));
			} else {
				list = selectDtoParams(sql, params);
			}
		}
		return (List<T>) list;
	}

	/**
	 * This method executes an ad hoc SQL statement without parameters.
	 * 
	 * @param sql
	 *            ad hoc SQL statment
	 * 
	 * @return List of DTOs
	 */
	public final List<T> select(final String sql) {
		return select(sql, null);
	}

	/**
	 * Return a list of DTOs using testParams to populate criteria. Use "select"
	 * as default statement.
	 * 
	 * @param params
	 *            the parameter list
	 * 
	 * @return List of DTOs
	 */
	@Override
	public final List<T> select(final P params) {
		return selectByStatement("select", params);
	}

	/**
	 * Persist DTO.
	 * 
	 * @param statement
	 *            the statement to lookup.
	 * @param dto
	 *            the DTO to persist.
	 * @return Records affected
	 */
	public final int insert(final String statement, final T dto) {
		return update(statement, dto);
	}

	/**
	 * Persist DTO. Use "insert" as default statement.
	 * 
	 * @param dto
	 *            the DTO to persist.
	 * @return Records affected
	 */
	@Override
	public final int insert(final T dto) {
		// Use update, so we don't have redundant code
		return insert("insert", dto);
	}

	/**
	 * This method executes a SQL insert, update or delete statement.
	 * 
	 * @param sql
	 *            The sql statement to execute.
	 * @param param
	 *            the DTO to update.
	 * @return Records affected
	 */
	@SuppressWarnings("unchecked")
	public final int execute(final String sql, final T param) {

		int recordsModified = 0;

		// if type is null, set it to ObjectUtils.NULL to avert NPE:
		final Class<?> daoType =
		    (Class<?>) ((type == null) ? ObjectUtils.NULL.getClass() : type);

		if (param == null) {
			recordsModified = jdbcTemplate.getJdbcOperations().update(sql);
		} else if (daoType.isInstance(param)) {
			// If DTO type use named parameters
			recordsModified = getJdbcTemplate().update(sql,
			    new BeanPropertySqlParameterSource(param));
		} else if (param instanceof Map<?, ?>) {
			// If Map type use named parameters
			recordsModified =
			    getJdbcTemplate().update(sql, (Map<String, ?>) param);

		} else if (param instanceof Object[]) {
			// If Object[] use parameter markers
			recordsModified =
			    getJdbcTemplate().getJdbcOperations().update(sql, param);
		} else {
			// If any other type throw exception
			throw new IllegalArgumentException(
			    "DTO must be of type " + daoType.getCanonicalName() + ", "
			        + Map.class.getCanonicalName() + " or "
			        + Object[].class.getCanonicalName() + " type passed was "
			        + param.getClass().getCanonicalName());
		}
		return recordsModified;
	}

	/**
	 * This method executes a SQL insert, update or delete statement with no
	 * parameters.
	 * 
	 * @param sql
	 *            The sql statement to execute.
	 * @return Records affected
	 */
	public final int execute(final String sql) {

		return execute(sql, null);
	}

	/**
	 * Update DTO. This implementation can use a DTO, Map or Object[] to handle
	 * selective field updates or multiple records. Use "update" as default
	 * statement.
	 * 
	 * @param dto
	 *            the DTO to update.
	 * @return Records affected
	 */
	@Override
	public final int update(final T dto) {
		return update("update", dto);
	}

	/**
	 * Update DTO. This implementation can use a DTO, Map or Object[] to handle
	 * selective field updates or multiple records. Use "update" as default
	 * statement.
	 * 
	 * @param statementKey
	 *            The key of the {@link NamedParamJdbcTemplateDao#statementMap}
	 *            that points to the SQL statement to execute.
	 * @param dto
	 *            the DTO to update.
	 * @return Records affected
	 */
	public final int update(final String statementKey, final T dto) {
		final String sql = statementMap.get(statementKey);
		if (StringUtils.isEmpty(sql)) {
			throw new IllegalArgumentException(
			    String.format("No sql statement found for statement key \"%s\"",
			        statementKey));
		}
		return execute(sql, dto);
	}

	/**
	 * Executes an arbitrary SQL statement with no parameters. Use this method
	 * for DML e.g. CREATE, DROP, etc.
	 * 
	 * @param statementKey
	 *            The key of the {@link NamedParamJdbcTemplateDao#statementMap}
	 *            that points to the SQL statement to execute.
	 * @return Records affected
	 */
	public final int update(final String statementKey) {

		final String sql = statementMap.get(statementKey);
		if (StringUtils.isEmpty(sql)) {
			throw new IllegalArgumentException(
			    String.format("No sql statement found for statement key \"%s\"",
			        statementKey));
		}

		return execute(sql, null);
	}

	/**
	 * Delete based on testParams to populate criteria.
	 * 
	 * @param statementKey
	 *            the statement to lookup.
	 * @param dto
	 *            the DTO to delete,
	 * @return Records affected
	 */
	public final int delete(final String statementKey, final T dto) {

		final String sql = statementMap.get(statementKey);
		if (StringUtils.isEmpty(sql)) {
			throw new IllegalArgumentException(
			    String.format("No sql statement found for statement key \"%s\"",
			        statementKey));
		}

		return execute(sql, dto);
	}

	/**
	 * Delete based on testParams to populate criteria. Use "delete" as default
	 * statement.
	 * 
	 * @param dto
	 *            the DTO to delete,
	 * @return Records affected.
	 */
	@Override
	public final int delete(final T dto) {
		return delete("delete", dto);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<T> selectByStatement(String statementId) {

		return selectByStatement(statementId, null);

	}

}
