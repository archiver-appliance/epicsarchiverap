package org.epics.archiverappliance.engine.generic;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class ScopedLogger {
	public static class ParentSpec {
		protected ParentSpec(Logger logger, ScopedLogger parent)
		{
			this.logger = logger;
			this.parent = parent;
		}
		
		protected final Logger logger;
		protected final ScopedLogger parent;
	}
	
	public static ParentSpec parentScope(ScopedLogger scope)
	{
		return new ParentSpec(scope.m_logger, scope);
	}
	
	public static ParentSpec parentRoot(Logger logger)
	{
		return new ParentSpec(logger, null);
	}
	
	public ScopedLogger(ParentSpec parent_spec, String name)
	{
		m_logger = parent_spec.logger;
		m_parent = parent_spec.parent;
		m_name = name;
	}
	
	public void log(Level level, Object message)
	{
		log(level, message, null);
	}
	
	public void log(Level level, Object message, Throwable th)
	{
		if (m_logger.isEnabledFor(level)) {
			m_logger.log(level, buildMessage(message), th);
		}
	}
	
	public void debug(Object message)
	{
		log(Level.DEBUG, message);
	}
	
	public void info(Object message)
	{
		log(Level.INFO, message);
	}
	
	public void warn(Object message)
	{
		log(Level.WARN, message);
	}
	
	public void warn(Object message, Throwable th)
	{
		log(Level.WARN, message, th);
	}
	
	public void error(Object message)
	{
		log(Level.ERROR, message);
	}
	
	public void error(Object message, Throwable th)
        {
                log(Level.ERROR, message, th);
        }
	
	public String buildMessage(Object message)
	{
		StringBuilder sb = new StringBuilder();
		build_context_recurser(sb);
		sb.append(": ");
		sb.append(message.toString());
		return sb.toString();
	}
	
	protected void build_context_recurser(StringBuilder sb)
	{
		if (m_parent != null) {
			m_parent.build_context_recurser(sb);
			sb.append(".");
		}
		sb.append(m_name);
	}
	
	protected Logger m_logger;
	protected ScopedLogger m_parent;
	protected String m_name;
}
