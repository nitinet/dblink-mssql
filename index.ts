import { Handler, model, sql } from 'dblink-core';
import mssql from 'mssql';
import { Readable } from 'stream';

/**
 * MsSqlServer Handler
 *
 * @export
 * @class MsSqlServer
 * @typedef {MsSqlServer}
 * @extends {Handler}
 */
export default class MsSqlServer extends Handler {
  /**
   * Connection Pool
   *
   * @type {mssql.ConnectionPool}
   */
  connectionPool: mssql.ConnectionPool;

  /**
   * Creates an instance of MsSqlServer.
   *
   * @constructor
   * @param {model.IConnectionConfig} config
   */
  constructor(config: model.IConnectionConfig) {
    super(config);

    this.connectionPool = new mssql.ConnectionPool({
      server: this.config.host,
      port: this.config.port,
      user: this.config.username,
      password: this.config.password,
      database: this.config.database
    });
  }

  /**
   * Get a new Connection
   *
   * @async
   * @returns {Promise<void>}
   */
  async init(): Promise<void> {
    await this.connectionPool.connect();
  }

  /**
   * Get a new Connection
   *
   * @async
   * @returns {Promise<mssql.Request>}
   */
  async getConnection(): Promise<mssql.Request> {
    await mssql.connect({
      server: this.config.host,
      port: this.config.port,
      user: this.config.username,
      password: this.config.password,
      database: this.config.database
    });
    return new mssql.Request();
  }

  /**
   * Initialize a Transaction
   *
   * @async
   * @param {mssql.Request} conn
   * @returns {Promise<void>}
   */
  async initTransaction(conn: mssql.Request): Promise<void> {
    /* document why this async method 'initTransaction' is empty */
  }

  /**
   * Commit Transaction
   *
   * @async
   * @param {mssql.Request} conn
   * @returns {Promise<void>}
   */
  async commit(conn: mssql.Request): Promise<void> {
    /* document why this async method 'commit' is empty */
  }

  /**
   * Rollback Transaction
   *
   * @async
   * @param {mssql.Request} conn
   * @returns {Promise<void>}
   */
  async rollback(conn: mssql.Request): Promise<void> {
    /* document why this async method 'rollback' is empty */
  }

  /**
   * Close Connection
   *
   * @async
   * @param {mssql.Request} conn
   * @returns {Promise<void>}
   */
  async close(conn: mssql.Request): Promise<void> {
    /* document why this async method 'close' is empty */
  }

  /**
   * Run string query
   *
   * @async
   * @param {string} query
   * @param {?any[]} [dataArgs]
   * @param {?mssql.Request} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  async run(query: string, dataArgs?: any[], connection?: mssql.Request): Promise<model.ResultSet> {
    let conn = connection ?? this.connectionPool.request();
    let data = await conn.query(query);

    let result = new model.ResultSet();
    result.rowCount = data.rowsAffected[0] ?? 0;
    result.rows = data.recordset;
    return result;
  }

  /**
   * Run statements
   *
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?mssql.Request} [connection]
   * @returns {Promise<model.ResultSet>}
   */
  runStatement(queryStmt: sql.Statement | sql.Statement[], connection?: mssql.Request): Promise<model.ResultSet> {
    let { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.run(query, dataArgs, connection);
  }

  /**
   * Run quries and stream output
   *
   * @async
   * @param {string} query
   * @param {?any[]} [dataArgs]
   * @param {?mssql.Request} [connection]
   * @returns {Promise<Readable>}
   */
  async stream(query: string, dataArgs?: any[], connection?: mssql.Request): Promise<Readable> {
    let conn = connection ?? this.connectionPool.request();
    conn.stream = true;

    conn.query(query);
    return conn.toReadableStream();
  }

  /**
   * Run statements and stream output
   *
   * @param {(sql.Statement | sql.Statement[])} queryStmt
   * @param {?mssql.Request} [connection]
   * @returns {Promise<Readable>}
   */
  streamStatement(queryStmt: sql.Statement | sql.Statement[], connection?: mssql.Request): Promise<Readable> {
    let { query, dataArgs } = this.prepareQuery(queryStmt);
    return this.stream(query, dataArgs, connection);
  }
}
