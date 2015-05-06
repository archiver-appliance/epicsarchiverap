package org.epics.archiverappliance.retrieval.postprocessors;

import java.util.LinkedList;

import org.apache.log4j.Logger;

/**
 * Factory class for post processors.
 * We use startsWith on the identity so we have to be careful when one post processor's identity is a substring of another. 
 * If possible, avoid
 * @author mshankar
 *
 */
public class PostProcessors {
	private static Logger logger = Logger.getLogger(PostProcessors.class.getName());
	
	private enum InheritValuesFromPreviousBins {DO_NOT_INHERIT, INHERIT};
	private static class PostProcessorImplementation {
		
		public PostProcessorImplementation(String key, Class<? extends PostProcessor> clazz) {
			this.key = key;
			this.clazz = clazz;
		}

		public PostProcessorImplementation(String key, Class<? extends PostProcessor> clazz, InheritValuesFromPreviousBins inheritValuesFromPreviousBins) {
			this.key = key;
			this.clazz = clazz;
			this.inheritValuesFromPreviousBins = inheritValuesFromPreviousBins;
		}
		
		String key;
		Class<? extends PostProcessor> clazz;
		private InheritValuesFromPreviousBins inheritValuesFromPreviousBins = InheritValuesFromPreviousBins.INHERIT;
	}
	
	private static LinkedList<PostProcessorImplementation> postprocessors = new LinkedList<PostProcessorImplementation>();
	
	private static void registerPostProcessor(String identity, Class<? extends PostProcessor> clazz) { 
		try {
			PostProcessor implementationInstance = clazz.newInstance();
			if(implementationInstance instanceof FillNoFillSupport) {
				logger.debug("Registering the no fill variant of post processor " + identity);
				postprocessors.add(new PostProcessorImplementation(identity + "Sample", clazz, InheritValuesFromPreviousBins.DO_NOT_INHERIT));
			}
		} catch (Exception e) {
			logger.error("Exception registering post processor " + identity, e);
		}
		
		// Now, register the actual post processor - in this case, the fill variant.
		postprocessors.add(new PostProcessorImplementation(identity, clazz));
	}
	
	static {
		// Order of insertion matters as we use startsWith; test when you add new post processors.
		registerPostProcessor(new FirstSamplePP().getIdentity(), FirstSamplePP.class);
		registerPostProcessor(new Mean().getIdentity(), Mean.class);
		registerPostProcessor(new Jitter().getIdentity(), Jitter.class);
		registerPostProcessor(new StandardDeviation().getIdentity(), StandardDeviation.class);
		registerPostProcessor(new IgnoreFliers().getIdentity(), IgnoreFliers.class);
		registerPostProcessor(new Fliers().getIdentity(), Fliers.class);
		registerPostProcessor(new LinearInterpolation().getIdentity(), LinearInterpolation.class);
		registerPostProcessor(new LoessInterpolation().getIdentity(), LoessInterpolation.class);
		registerPostProcessor(new Median().getIdentity(), Median.class);
		registerPostProcessor(new Variance().getIdentity(), Variance.class);
		registerPostProcessor(new PopulationVariance().getIdentity(), PopulationVariance.class);
		registerPostProcessor(new Kurtosis().getIdentity(), Kurtosis.class);
		registerPostProcessor(new Skewness().getIdentity(), Skewness.class);
		registerPostProcessor(new FirstFill().getIdentity(), FirstFill.class);
		registerPostProcessor(new LastFill().getIdentity(), LastFill.class);
		registerPostProcessor(new LastSample().getIdentity(), LastSample.class);
		registerPostProcessor(new Max().getIdentity(), Max.class);
		registerPostProcessor(new Min().getIdentity(), Min.class);
		registerPostProcessor(new Count().getIdentity(), Count.class);
		registerPostProcessor(new NCount().getIdentity(), NCount.class);
		registerPostProcessor(new Nth().getIdentity(), Nth.class);
	}

	public static PostProcessor findPostProcessor(String postProcessorUserArg) {
		if(postProcessorUserArg != null) {
			try {
				for(PostProcessorImplementation implementation : postprocessors) {
					if(postProcessorUserArg.startsWith(implementation.key)) {
						logger.debug("Found postprocessor for " + postProcessorUserArg);
						PostProcessor implementationInstance = implementation.clazz.newInstance();
						if(implementation.inheritValuesFromPreviousBins == InheritValuesFromPreviousBins.DO_NOT_INHERIT 
								&& implementationInstance instanceof FillNoFillSupport) { 
							logger.debug("Turning off inheriting values from previous bins for empty bins");
							((FillNoFillSupport)implementationInstance).doNotInheritValuesFromPrevioisBins();
						}
						return implementationInstance;
					}
				}
			} catch(Exception ex) {
				logger.error("Exception initializing processor", ex);
			}
			logger.error("Did not find post processor for " + postProcessorUserArg);
		}
		return null;
	}


	public static final int DEFAULT_SUMMARIZING_INTERVAL = 15*60;
}
