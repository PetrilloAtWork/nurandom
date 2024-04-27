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
        NAlgos,                          ///< total number of seed algorithms
        saUndefined,                     ///< algorithm not defined
        saDefault = saEventTimestamp_v1  ///< default algorithm
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
       * - *algorithm* (string, default: "EventTimestamp_v1"): the name of the
       *   algorithm to get the seed
       * - *offset* (integer, optional): if specified, all the final seeds are
       *   incremented by this value; the resulting seed is not checked, it
       *   might even be invalid. This is considered an emergency hack when
       *   one absolutely needs to have a different seed than the one assigned
       *   to the event. This also defies the purpose of the policy, since after
       *   this, to reproduce the random sequences the additional knowledge of
       *   which offset was used is necessary.
       * - *processName* (string, optional): if specified, the specified process
       *   name is used instead of the one from the current process.
       */
      virtual void configure(fhicl::ParameterSet const& pset) override;
      
      /// Prints the details of the configuration of the random generator
      virtual void print(std::ostream& out) const override;
      
      
      /// Default algorithm version
      static constexpr const char* DefaultVersion = "v1";
      
      
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
       *   value, used for the seed
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
      MF_LOG_DEBUG("PerEventPolicy") << "Seed from: '" << s << "': " << seed;
      return seed;
    } // PerEventPolicy<SEED>::EventTimestamp_v1()
    
    
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
      
      // EventTimestamp_v1 does not require specific configuration
      
      
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
      seed_t seed = base_t::InvalidSeed;
      EventData_t myInfo{ info };
      if (processName) myInfo.processName = *processName;
      switch (algo) {
        case saEventTimestamp_v1:
          seed = EventTimestamp_v1(id, myInfo);
          break;
        case saUndefined:
          throw art::Exception(art::errors::Configuration)
            << "Per-event random number seeder not configured!\n";
        default:
          throw art::Exception(art::errors::LogicError)
            << "Unsupported per-event random number seeder (#"
            << ((int) algo) << ")\n";
      } // switch
      return seed + offset;
    } // PerEventPolicy<SEED>::createEventSeed()
    
    
  } // namespace details
  
} // namespace rndm


#endif // NURANDOM_RANDOMUTILS_PROVIDERS_PEREVENTPOLICY_H
