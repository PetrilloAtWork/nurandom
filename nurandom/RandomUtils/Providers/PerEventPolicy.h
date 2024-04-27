/**
 * @file   PerEventPolicy.h
 * @brief  Implementation of the per-event random seed assignment policy
 * @author Gianluca Petrillo (petrillo@fnal.gov)
 * @date   20150211
 * @see    SeedMaster.h
 * 
 * No code in this files is directly serviceable.
 * Documentation is up to date though.
 */

#ifndef NURANDOM_RANDOMUTILS_PROVIDERS_PEREVENTPOLICY_H
#define NURANDOM_RANDOMUTILS_PROVIDERS_PEREVENTPOLICY_H 1

// C/C++ standard libraries
#include <string>
#include <optional>
#include <filesystem> // std::filesystem::path
#include <memory> // std::unique_ptr<>
#include <type_traits> // std::make_signed<>

// From art and its tool chain
#include "messagefacility/MessageLogger/MessageLogger.h"
#include "fhiclcpp/ParameterSet.h"
#include "canvas/Utilities/Exception.h"

// Some helper classes
#include "nurandom/RandomUtils/Providers/PolicyFactory.h" // makeRandomSeedPolicy
#include "nurandom/RandomUtils/Providers/RandomSeedPolicyBase.h"
#include "nurandom/RandomUtils/Providers/EngineId.h"
#include "nurandom/RandomUtils/Providers/EventSeedInputData.h"


namespace rndm {
  
  namespace details {
    
    /** ************************************************************************
     * @brief Implementation of the "perEvent" policy
     *
     * This policy extracts seeds depending on contextual information from the
     * event.
     * The information that enters the seed is the event ID
     * (run, subrun, event), the process name, and the engine ID.
     * 
     * The policy is only effective if an event is being processed.
     * Before the first event is processed, seeds are initialized to a fixed
     * value, while in between the events they are not modified and the random
     * numbers extracted at that time will depend on which event was processed
     * last.
     * 
     * As a partial mitigation to this, it is possible to specify a "pre-event"
     * policy that is used to initialize the random engines on construction,
     * just like the policies which do not depend on the event (like
     * `autoIncrement` and `random`) do. This is achieved by specifying in the
     * `initSeedPolicy` configuration table the whole configuration of this
     * "fallback" policy. For example:
     * ~~~~
     *   NuRandomService: {
     *     policy            : "perEvent"
     *     
     *     initSeedPolicy: {
     *       policy            : "preDefinedSeed"
     *       baseSeed          :     1
     *       maxUniqueEngines  :     6
     *       checkRange        :  true
     *       Module1: { a : 3  b : 5 }
     *       Module2: { a : 7  c : 9 }
     *     } # initSeedPolicy
     *     
     *     verbosity         :     2
     *     endOfJobSummary   :  false
     * 
     *   }
     * ~~~~
     * sets up the `perEvent` policy, and uses a `preDefinedSeed` for the seeds
     * before the first event.
     * 
     * 
     * Configuration parameters
     * -------------------------
     * 
     * See the documentation of `configure()`.
     * 
     * 
     * Special use cases
     * ==================
     * 
     * The `perEvent` policy is expected to provide reliably reproducible random
     * streams when extractions happen within the event processing (`produce()`,
     * `filter()`, `analyze()`, etc.) and the job configuration is exactly the
     * same.
     * 
     * Job configuration changes
     * --------------------------
     * 
     * As long as the process name and the label of a module are not changed,
     * the streams of that module are reproducible. Note however that if a new
     * job has the module behaviour change, because of updated code or changed
     * configuration, the same random sequence may end up used differently and
     * still yield different results in the module and downstream of it.
     * 
     * 
     * Process name changes
     * ---------------------
     * 
     * A possible user pattern is for a workflow of processes `A`, `B` and `C`
     * to take place, and its output saved by `RootOutput` module.
     * Then, either for bug fix or for systematic variations, we may want to
     * repeat `B` and then `C` on top of the result of `A`, and chances are that
     * the only available sample is the output after `C` job.
     * In that case, using `C` output files as `RootInput` input, dropping
     * all `B` and `C` data products and running the same (or amended)
     * configurations is not allowed by _art_ because process names `B` and `C`
     * have already been seen. The solution of changing the name of the
     * processes, e.g. into `Bfix1` and `Cfix1`, will allow processing, but then
     * the random streams will be different from `B` and `C` because of the new
     * process name.
     * 
     * To work around this issue it is possible to tell `perEvent` policy to use
     * a given process name, instead of the one of the current process, to
     * construct the seeds for the random streams. Specifying ad-hoc
     * configurations like:
     *     
     *     #include "jobB.fcl"
     *     
     *     process_name: Bfix1
     *     services.NuRandomService.processName: B
     *     
     * will recover the streams as in the original process `B`.
     * 
     * 
     * Modules extracting random numbers outside the event processing
     * ---------------------------------------------------------------
     * 
     * `perEvent` policy can set random seeds on the start of each event, thus
     * guaranteeing consistency of random stream within each event.
     * To do that it builds the seed based on a context that includes event
     * information, process name and module name.
     * To regulate random number extraction outside the event processing, where
     * such context can't be established, the `initSeedPolicy` complementary
     * policy can be set. This has all the disadvantages of the policy that is
     * chosen as the complementary, typically including the need of strict
     * bookkeeping that almost offsets the gains from `perEvent` policy.
     * 
     * An example of this behaviour is the CORSIKA generator module, which
     * (at the time of writing) randomly decides which database portion to use
     * at construction time. At that point, only the `initSeedPolicy` is active.
     * 
     * Any mitigation requires the determination of a context which is unique
     * for each job, and independent of event, and also of run and subrun if
     * we want it to work before runs and subruns are available.
     * Uniqueness is not trivial especially in generated samples, which often
     * have run and event numbers repeated between samples and even within the
     * same sample.
     * 
     * The following mitigation includes in the context the input file name.
     * Therefore, only before the first event in a file, streams are initialized
     * with seeds depending on the current file name. This approach can be
     * enabled by configuring the policy with `algorithm: EventTimestamp_v2`.
     * 
     * Clearly there are a lot of limitations to this approach, and users need
     * to understand them:
     * 
     * 1. The mitigation affects only the time from the opening of the input
     *    file to the processing of the first event. At that time, all engines
     *    are reseeded (the system does not have the concept of whether the
     *    random stream is used in the event processing or outside it).
     *    Because _art_ marks a run boundary when the input file changes,
     *    the module calls that can take advantage of this mitigation are
     *    `respondToOpenInputFile()`, `respondToOpenOutputFiles()`,
     *    and the first `beginRun()` and `beginSubRun()` calls on each file.
     * 2. Modules using random numbers in construction or `beginJob()` will
     *    still suffer a problem. As a workaround, **modules may be need to be
     *    redesigned** to do their thing in `respondToOpenInputFile()` instead.
     *    Note however that:
     *      * `beginRun()` and `beginSubRun()` are not ideal, because they can
     *        happen also within the same file, in which case they will find
     *        a random stream set up with the seed of the previous event.
     *      * The code should react correctly to _all_
     *        `respondToOpenInputFile()`, so that if there is a second input
     *        file, seeds for processing the events it contains will be set
     *        taking into account its name.
     *    
     *    Clearly this constrain may have unwanted consequences. For example,
     *    in the case of CORSIKA, a new database file should be read for each
     *    input file.
     * 3. This mitigation may work only if there is an input file. Input systems
     *    without a unique file name won't trigger it. Jobs with input files
     *    from a database like SAM can work since the file names are unique in
     *    the database. Generation events that get their input from `EmptyEvent`
     *    will _not_ take advantage of this feature either. In this case, the
     *    recommended approach is to introduce a preliminary stage (`Empty`)
     *    that executes only `EmptyEvent` and `RootOutput`, and assigns a
     *    timestamp to the events so that the regular `perEvent` policy can
     *    work. The generation events will use such events as input: the events
     *    and their files will provide all the context needed for reproducible
     *    random streams.
     * 4. Skipping input events is supported by this workaround.
     * 
     */
    template <typename SEED>
    class PerEventPolicy: public RandomSeedPolicyBase<SEED> {
        public:
      using base_t = RandomSeedPolicyBase<SEED>;
      using this_t = PerEventPolicy<SEED>;
      using seed_t = typename base_t::seed_t;
      
      /// type for contextual event information
      using EventData_t = NuRandomServiceHelper::EventSeedInputData;
      
      typedef enum {
        saEventTimestamp_v1,             ///< event timestamp algorithm (v1)
        saEventTimestamp_v2,             ///< event timestamp algorithm (v2)
        NAlgos,                          ///< total number of seed algorithms
        saUndefined,                     ///< algorithm not defined
        saDefault = saEventTimestamp_v2  ///< default algorithm
      } SeedAlgo_t; ///< seed algorithms; see reseed() documentation for details
      
      /// Configures from a parameter set
      /// @see configure()
      PerEventPolicy(fhicl::ParameterSet const& pset): base_t("perEvent")
        { this_t::configure(pset); }
      
      /// Returns whether the returned seed should be unique: for us it "no".
      virtual bool yieldsUniqueSeeds() const override { return false; }
      
      
      /**
       * @brief Configure this policy
       * @param pset the parameter set for the configuration
       * 
       * Parameters:
       * - *policy* (string): needs to be `perEvent`.
       * - *algorithm* (string; default: `default`): the algorithm used to build
       *   the seeds. The `default` value maps to `saDefault`.
       *   The algorithms are described in `createEventSeed()`, and some usage
       *   directions are in the `PerEventPolicy` class description.
       * - *offset* (integer, optional): if specified, all the final seeds are
       *   incremented by this value; the resulting seed is not checked, it
       *   might even be invalid. This is considered an emergency hack when
       *   one absolutely needs to have a different seed than the one assigned
       *   to the event. This also defies the purpose of the policy, since after
       *   this, to reproduce the random sequences the additional knowledge of
       *   which offset was used is necessary.
       * - *initSeedPolicy* (configuration table, optional): the configuration
       *   of another random seed policy to be used outside of the event scope
       *   (see class documentation for usage directions).
       * - *processName* (string, optional): if specified, the specified process
       *   name is used instead of the one from the current process.
       * 
       */
      virtual void configure(fhicl::ParameterSet const& pset) override;
      
      /// Prints the details of the configuration of the random generator
      virtual void print(std::ostream& out) const override;
      
      
      /// Default algorithm version
      static constexpr const char* DefaultVersion = "v2";
      
      
      /// Converts some information into a valid seed by means of hash values
      template <typename Hashable>
      static seed_t SeedFromHash(Hashable const& info)
        { return makeValid(std::hash<Hashable>()(info)); }
      
      /// Converts run, subrun and event numbers into a string
      static std::string UniqueEventIDString(EventData_t const& info);
      
      /// Converts event ID and timestamp information into a string
      static std::string UniqueEventString(EventData_t const& info);
      
      
        private:
      
      /// type for seed offset
      using SeedOffset_t = typename std::make_signed<seed_t>::type;
      
      SeedAlgo_t algo; ///< the algorithm to extract the seed
      
      SeedOffset_t offset; ///< offset added to all the seeds
      
      std::optional<std::string> processName; ///< override process name
      
      /// Policy used for initialization before the event (none by default).
      PolicyStruct_t<seed_t> initSeedPolicy;
      
      /// Per-job seed: pre-event seeds are returned (or invalid if none).
      virtual seed_t createSeed(SeedMasterHelper::EngineId const& id) override;
      
      /**
       * @brief Returns a seed proper for the specified event information
       * @param id random number engine ID (moule label and instance name)
       * @param info event information
       * @return a seed specific to the information provided
       *
       * The algorithm used to combine the provided information into a seed is
       * defined by the configuration. The following algorithms are supported:
       * - *EventTimestamp_v1*: includes event ID (run, subrun and event
       *   numbers), event timestamp, process name and engine ID into a hash
       *   value, used for the seed.
       * - *EventTimestamp_v2*: includes event ID and timestamp, like
       *   `EventTimestamp_v1`, if the event ID is valid, otherwise it includes
       *   the input file basename (which can be empty); and then process name
       *   and engine ID. All into a hash value, used for the seed.
       *   When event ID is available, it's equivalent to `EventTimestamp_v1`.
       */
      virtual seed_t createEventSeed
        (SeedMasterHelper::EngineId const& id, EventData_t const& info)
        override;
      
      /// Renders a seed valid
      template <typename T>
      static seed_t makeValid(T value) { return ValidSeed<seed_t>(value); }
      
      /// Implementation of the EventTimestamp_v1 algorithm
      static seed_t EventTimestamp_v1
        (SeedMasterHelper::EngineId const& id, EventData_t const& info);
      
      /// Implementation of the EventTimestamp_v2 algorithm
      static seed_t EventTimestamp_v2
        (SeedMasterHelper::EngineId const& id, EventData_t const& info);
      
      /// Returns the name of the object pointed by the path.
      static std::string basename(std::filesystem::path const& path)
        { return path.filename(); }
      
      //@{
      /// Algorithm name (manual) handling
      static const std::vector<std::string> algoNames;
      
      static std::vector<std::string> InitAlgoNames();
      //@}
    }; // class PerEventPolicy<>
    
    
    
    //--------------------------------------------------------------------------
    //---  PerEventPolicy template implementation
    //---
    template <typename SEED>
    std::vector<std::string> PerEventPolicy<SEED>::InitAlgoNames() {
      
      std::vector<std::string> names((size_t) NAlgos);
      
      names[saEventTimestamp_v1] = "EventTimestamp_v1";
      names[saEventTimestamp_v2] = "EventTimestamp_v2";
      
      return names;
    } // PerEventPolicy<SEED>::InitAlgoNames()
    
    
    template <typename SEED>
    const std::vector<std::string> PerEventPolicy<SEED>::algoNames
      = PerEventPolicy<SEED>::InitAlgoNames();
    
    
    //--------------------------------------------------------------------------
    template <typename SEED>
    std::string PerEventPolicy<SEED>::UniqueEventIDString
      (EventData_t const& info)
    {
      return "Run: " + std::to_string(info.runNumber)
        + " Subrun: " + std::to_string(info.subRunNumber)
        + " Event: "  + std::to_string(info.eventNumber)
        ;
    } // PerEventPolicy<SEED>::UniqueEventIDString()
    
    
    template <typename SEED>
    std::string PerEventPolicy<SEED>::UniqueEventString
      (EventData_t const& info)
    {
      return UniqueEventIDString(info)
        + " Timestamp: " + std::to_string(info.time);
    } // PerEventPolicy<SEED>::UniqueEventString()
    
    
    template <typename SEED>
    auto PerEventPolicy<SEED>::EventTimestamp_v1
      (SeedMasterHelper::EngineId const& id, EventData_t const& info)
      -> seed_t
    {
      // this version does not provide seeds without an actual event
      if (!info.isEventValid()) return base_t::InvalidSeed;
      if (!info.isTimeValid) {
        throw art::Exception(art::errors::InvalidNumber)
          << "Input event has an invalid timestamp,"
          " random seed per-event policy EventTimestamp_v1 can't be used.\n";
      }
      std::string s = UniqueEventString(info)
        + " Process: " + info.processName
        + " Module: " + id.moduleLabel;
      if (!id.instanceName.empty())
        s.append(" Instance: ").append(id.instanceName);
      seed_t seed = SeedFromHash(s);
      MF_LOG_DEBUG("PerEventPolicy")
        << "[EventTimestamp_v1] Seed from: '" << s << "': " << seed;
      return seed;
    } // PerEventPolicy<SEED>::EventTimestamp_v1()
    
    
    template <typename SEED>
    auto PerEventPolicy<SEED>::EventTimestamp_v2
      (SeedMasterHelper::EngineId const& id, EventData_t const& info)
      -> seed_t
    {
      if (info.isEventValid() && !info.isTimeValid) {
        throw art::Exception(art::errors::InvalidNumber)
          << "Input event has an invalid timestamp,"
          " random seed per-event policy EventTimestamp_v2 can't be used.\n";
      }
      std::string s = (
          info.isEventValid()
          ? UniqueEventString(info)
          : "Input: " + basename(info.inputFileName)
        )
        + " Process: " + info.processName
        + " Module: " + id.moduleLabel;
      if (!id.instanceName.empty())
        s.append(" Instance: ").append(id.instanceName);
      seed_t const seed = SeedFromHash(s);
      MF_LOG_DEBUG("PerEventPolicy")
        << "[EventTimestamp_v2] Seed from: '" << s << "': " << seed;
      return seed;
    } // PerEventPolicy<SEED>::EventTimestamp_v2()
    
    
    //--------------------------------------------------------------------------
    template <typename SEED>
    void PerEventPolicy<SEED>::configure(fhicl::ParameterSet const& pset) {
      // set the per-event algorithm
      algo = saUndefined;
      std::string algorithm_name
         = pset.get<std::string>("algorithm", "default");
      
      if (algorithm_name == "default") algo = saDefault;
      else {
        for (size_t iAlgo = 0; iAlgo < (size_t) NAlgos; ++iAlgo) {
          if (algorithm_name != algoNames[iAlgo]) continue;
          algo = (SeedAlgo_t) iAlgo;
          break;
        } // for
      }
      if (algo == saUndefined) {
        throw art::Exception(art::errors::Configuration)
          << "No valid event random seed algorithm specified!\n";
      }
      
      // read an optional overall offset
      offset = pset.get<SeedOffset_t>("offset", 0);
      
      processName = pset.has_key("processName")
        ? std::make_optional(pset.get<std::string>("processName"))
        : std::nullopt;
      
      // EventTimestamp_v1/2 does not require specific configuration
      
      
      // set the pre-event algorithm
      auto const& initSeedConfig
        = pset.get<fhicl::ParameterSet>("initSeedPolicy", {});
      if (!initSeedConfig.is_empty()) {
        try {
          initSeedPolicy = makeRandomSeedPolicy<seed_t>(initSeedConfig);
        }
        catch(cet::exception const& e) {
          throw cet::exception{ "PerEventPolicy", "", e }
            << "Error creating the pre-event policy of `perEvent` random policy"
            " from configuration:\n"
            << initSeedConfig.to_indented_string(2);
        }
      } // if pre-event policy
      
    } // PerEventPolicy<SEED>::configure()
    
    
    //--------------------------------------------------------------------------
    /// Prints the details of the configuration of the random generator
    template <typename SEED>
    void PerEventPolicy<SEED>::print(std::ostream& out) const {
      base_t::print(out);
      out
        << "\n  algorithm version: " << algoNames[algo];
      if (offset != 0)
        out << "\n  constant offset:   " << offset;
      out << "\n  process name:      " << processName.value_or("from the job");
      if (initSeedPolicy) {
        out << "\n  special policy for random seeds before the event: '"
          << policyName(initSeedPolicy.policy)
          << "'\n" << std::string(60, '-');
        initSeedPolicy->print(out);
        out << "\n" << std::string(60, '-');
      }
    } // PerEventPolicy<SEED>::print()
    
    
    //--------------------------------------------------------------------------
    template <typename SEED>
    typename PerEventPolicy<SEED>::seed_t PerEventPolicy<SEED>::createSeed
      (SeedMasterHelper::EngineId const& id)
    { return initSeedPolicy? initSeedPolicy->getSeed(id): base_t::InvalidSeed; }
    
    
    //--------------------------------------------------------------------------
    template <typename SEED>
    typename PerEventPolicy<SEED>::seed_t PerEventPolicy<SEED>::createEventSeed
        (SeedMasterHelper::EngineId const& id, EventData_t const& info)
    {
      // this function may be called from two different contexts: when an event
      // is being processed, or when no event is processed but an input source
      // has been open
      seed_t seed = base_t::InvalidSeed;
      EventData_t myInfo{ info };
      if (processName) myInfo.processName = *processName;
      switch (algo) {
        case saEventTimestamp_v1:
          // if we are out of the event, this algo will fail and return invalid
          seed = EventTimestamp_v1(id, myInfo);
          break;
        case saEventTimestamp_v2:
          seed = EventTimestamp_v2(id, myInfo);
          break;
        case saUndefined:
          throw art::Exception(art::errors::Configuration)
            << "Per-event random number seeder not configured!\n";
        default:
          throw art::Exception(art::errors::LogicError)
            << "Unsupported per-event random number seeder (#"
            << ((int) algo) << ")\n";
      } // switch
      
      return (seed == base_t::InvalidSeed)? seed: seed + offset;
    } // PerEventPolicy<SEED>::createEventSeed()
    
    
  } // namespace details
  
} // namespace rndm


#endif // NURANDOM_RANDOMUTILS_PROVIDERS_PEREVENTPOLICY_H
