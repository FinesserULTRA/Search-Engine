import { useState, useRef, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { MapPin, Search } from "lucide-react";
import { useNavigate } from "react-router-dom";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import { globalCss } from "@stitches/react";

const globalStyles = globalCss({
    "*:focus": {
        outline: "none !important",
        boxShadow: "none !important",
    },
});

const locations = [
    {
        "value": "new york city",
        "label": "New York City"
    },
    {
        "value": "los angeles",
        "label": "Los Angeles"
    },
    {
        "value": "las vegas",
        "label": "Las Vegas"
    },
    {
        "value": "chicago",
        "label": "Chicago"
    },
    {
        "value": "miami",
        "label": "Miami"
    },
    {
        "value": "orlando",
        "label": "Orlando"
    },
    {
        "value": "san francisco",
        "label": "San Francisco"
    },
    {
        "value": "houston",
        "label": "Houston"
    },
    {
        "value": "washington dc",
        "label": "Washington DC"
    },
    {
        "value": "dallas",
        "label": "Dallas"
    },
    {
        "value": "san diego",
        "label": "San Diego"
    },
    {
        "value": "seattle",
        "label": "Seattle"
    },
    {
        "value": "atlanta",
        "label": "Atlanta"
    },
    {
        "value": "boston",
        "label": "Boston"
    },
    {
        "value": "philadelphia",
        "label": "Philadelphia"
    },
    {
        "value": "denver",
        "label": "Denver"
    },
    {
        "value": "phoenix",
        "label": "Phoenix"
    },
    {
        "value": "austin",
        "label": "Austin"
    },
    {
        "value": "san antonio",
        "label": "San Antonio"
    },
    {
        "value": "fort worth",
        "label": "Fort Worth"
    },
    {
        "value": "charlotte",
        "label": "Charlotte"
    },
    {
        "value": "columbus",
        "label": "Columbus"
    },
    {
        "value": "detroit",
        "label": "Detroit"
    },
    {
        "value": "baltimore",
        "label": "Baltimore"
    },
    {
        "value": "jacksonville",
        "label": "Jacksonville"
    },
    {
        "value": "indianapolis",
        "label": "Indianapolis"
    },
    {
        "value": "el paso",
        "label": "El Paso"
    },
    {
        "value": "memphis",
        "label": "Memphis"
    },
    {
        "value": "san jose",
        "label": "San Jose"
    }
]



const hotelClasses = [
    { value: "all", label: "Any Class" },
    { value: "1", label: "1 Star" },
    { value: "2", label: "2 Stars" },
    { value: "3", label: "3 Stars" },
    { value: "4", label: "4 Stars" },
    { value: "5", label: "5 Stars" },
];

const SearchBar = () => {
    const navigate = useNavigate();
    globalStyles();
    const [searchTerm, setSearchTerm] = useState("");
    const [locationSearchTerm, setLocationSearchTerm] = useState("");
    const [filteredLocations, setFilteredLocations] = useState(locations);
    const [selectedLocation, setSelectedLocation] = useState(null);
    const [selectedHotelClass, setSelectedHotelClass] = useState("all");
    const [isSelectOpen, setIsSelectOpen] = useState(false);
    const locationInputRef = useRef(null);

    const handleSearch = () => {
        const query = `${searchTerm}`;
        const locationQuery = selectedLocation && selectedLocation !== 'all' ? `&location=${selectedLocation}` : '';
        const hotelClassQuery = selectedHotelClass && selectedHotelClass !== 'all' ? `&hotel_class=${selectedHotelClass}` : '';

        fetch(`http://127.0.0.1:8000/search?query=${query}${locationQuery}${hotelClassQuery}&doc_type=all`, {
            method: "GET",
        })
            .then((res) => res.json())
            .then((data) => {
                console.log(data);
                navigate("/search", { state: { data } });
            })
            .catch((err) => {
                console.log(err);
            });
    }

    useEffect(() => {
        if (isSelectOpen && locationInputRef.current) {
            locationInputRef.current.focus();
        }
    }, [isSelectOpen]);

    const handleSearchChange = (event) => {
        setSearchTerm(event.target.value);
    };

    const handleLocationSearchChange = (event) => {
        const value = event.target.value;
        setLocationSearchTerm(value);
        const filtered = locations.filter((location) =>
            location.label.toLowerCase().includes(value.toLowerCase())
        );
        setFilteredLocations(filtered);
    };

    const handleLocationSelect = (value) => {
        setSelectedLocation(value);
        setLocationSearchTerm("");
        setFilteredLocations(locations);
    };

    return (
        <div className="flex flex-col sm:flex-row w-full max-w-3xl mx-auto border-2 border-secondaryclr bg-primaryclr rounded-2xl sm:rounded-full shadow-sm overflow-hidden">
            <Input
                type="text"
                placeholder="Search your preferred hotel"
                value={searchTerm}
                onChange={handleSearchChange}
                className="flex-1 font-sans border-none text-secondaryclr h-12 px-6 bg-primaryclr focus:outline-none focus:ring-0 focus:ring-offset-0 focus-visible:ring-0 focus-visible:ring-offset-0 focus-visible:outline-none"
            />
            <div className="hidden sm:block border-l-2 border-l-secondaryclr h-6 mx-2 self-center" />
            <div className="flex items-center justify-between sm:justify-start w-full sm:w-auto">
                <Select
                    onOpenChange={setIsSelectOpen}
                    onValueChange={handleLocationSelect}
                    value={selectedLocation || undefined}
                >
                    <SelectTrigger className="w-full sm:w-40 border-none bg-primaryclr focus:outline-none focus:border-none focus:ring-0 focus:ring-offset-0 flex items-center space-x-2 h-12 px-6">
                        <MapPin className="h-4 w-4 text-secondaryclr" />
                        <SelectValue placeholder="Location" />
                    </SelectTrigger>
                    <SelectContent className="max-h-60 max-w overflow-y-auto">
                        <div className="p-2">
                            <Input
                                type="text"
                                placeholder="Search location"
                                value={locationSearchTerm}
                                onChange={handleLocationSearchChange}
                                className="mb-2 focus:outline-none focus:ring-0 focus:ring-offset-0 focus-visible:ring-0 focus-visible:ring-offset-0 focus-visible:outline-none"
                                ref={locationInputRef}
                                onKeyDown={(e) => e.stopPropagation()}
                            />
                        </div>
                        {filteredLocations.map((location) => (
                            <SelectItem key={location.value} value={location.value}>
                                {location.label}
                            </SelectItem>
                        ))}
                        {filteredLocations.length === 0 && (
                            <div className="p-2 text-sm text-gray-500">
                                No locations found
                            </div>
                        )}
                    </SelectContent>
                </Select>
                <div className="hidden sm:block border-l-2 border-l-secondaryclr h-6 mx-2 self-center" />
                <Select
                    onValueChange={setSelectedHotelClass}
                    value={selectedHotelClass}
                >
                    <SelectTrigger className="w-full sm:w-32 border-none bg-primaryclr focus:outline-none focus:border-none focus:ring-0 focus:ring-offset-0 flex items-center space-x-2 h-12 px-6">
                        <SelectValue placeholder="Hotel Class" />
                    </SelectTrigger>
                    <SelectContent className="max-h-60 overflow-y-auto">
                        {hotelClasses.map((hotelClass) => (
                            <SelectItem key={hotelClass.value} value={hotelClass.value}>
                                {hotelClass.label}
                            </SelectItem>
                        ))}
                    </SelectContent>
                </Select>
                <div className="hidden sm:block border-l-2 border-l-secondaryclr h-6 mx-2 self-center" />
                <Button
                    variant="ghost"
                    size="icon"
                    className="h-12 w-12 text-secondaryclr hover:bg-transparent hover:border-none duration-200 focus:outline-none focus:ring-0"
                    onClick={() => handleSearch()}
                >
                    <Search className="h-5 w-5" />
                    <span className="sr-only">Search</span>
                </Button>
            </div>
        </div>
    );
};

export default SearchBar;
