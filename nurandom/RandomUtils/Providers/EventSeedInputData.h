/**
 * @file   EventSeedInputData.h
 * @brief  A data object holding enough data to define a event seed
 * @author Gianluca Petrillo (petrillo@fnal.gov)
 * @date   February 18th, 2015
 */


#ifndef NURANDOM_RANDOMUTILS_PROVIDERS_EVENTSEEDINPUTDATA_H
#define NURANDOM_RANDOMUTILS_PROVIDERS_EVENTSEEDINPUTDATA_H 1

// C/C++ standard libraries
#include <cstdint> // std::uint32_t
#include <string>


namespace rndm {
  namespace NuRandomServiceHelper {
    
    /// Simple data structure with data needed to extract a seed from a event
    class EventSeedInputData {
        public:
      using RunNumber_t    = std::uint32_t;
      using SubRunNumber_t = std::uint32_t;
      using EventNumber_t  = std::uint32_t;
      using TimeValue_t    = std::uint64_t;
      
      /// @{
      /// @name Public data members
      
      RunNumber_t    runNumber = 0;    ///< run number
      SubRunNumber_t subRunNumber = 0; ///< subrun number
      EventNumber_t  eventNumber = 0;  ///< event number
      TimeValue_t    time = 0;         ///< event time
      
      bool           isData = false;   ///< whether processing real data
      
      std::string processName;      ///< name of the running process
      std::string moduleType;       ///< name of the class of the running module
      std::string moduleLabel;      ///< label of the running module instance
      
      bool isTimeValid = false;        ///< whether timestamp is valid
      /// @}
      
      
      /// Resets all the fields
      void clear() { *this = EventSeedInputData{}; }
      
    }; // class EventSeedInputData
  } // namespace NuRandomServiceHelper
} // namespace rndm


#endif // NURANDOM_RANDOMUTILS_PROVIDERS_EVENTSEEDINPUTDATA_H
