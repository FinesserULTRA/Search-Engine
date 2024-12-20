import { useState, useRef, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { MapPin, Search } from 'lucide-react';
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
    { value: "all", label: "Anywhere" },
    { value: "new-york", label: "New York" },
    { value: "london", label: "London" },
    { value: "paris", label: "Paris" },
    { value: "tokyo", label: "Tokyo" },
    { value: "berlin", label: "Berlin" },
    { value: "rome", label: "Rome" },
    { value: "madrid", label: "Madrid" },
    { value: "amsterdam", label: "Amsterdam" },
    { value: "barcelona", label: "Barcelona" },
    { value: "vienna", label: "Vienna" },
];

const SearchBar = () => {
    globalStyles();
    const [searchTerm, setSearchTerm] = useState("");
    const [locationSearchTerm, setLocationSearchTerm] = useState("");
    const [filteredLocations, setFilteredLocations] = useState(locations);
    const [selectedLocation, setSelectedLocation] = useState(null);
    const [isSelectOpen, setIsSelectOpen] = useState(false);
    const locationInputRef = useRef(null);

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
        <div className="flex w-max-screen items-center border-2 border-secondaryclr bg-primaryclr rounded-full shadow-sm">
            <Input
                type="text"
                placeholder="Search your preferred hotel"
                value={searchTerm}
                onChange={handleSearchChange}
                className="flex-1 font-sans border-none text-secondaryclr rounded-l-full h-12 pl-6 bg-primaryclr focus:outline-none focus:ring-0 focus:ring-offset-0 focus-visible:ring-0 focus-visible:ring-offset-0 focus-visible:outline-none"
            />
            <div className="border-l-2 border-l-secondaryclr h-6 mx-2" />
            <Select onOpenChange={setIsSelectOpen} onValueChange={handleLocationSelect} value={selectedLocation || undefined}>
                <SelectTrigger className="w-max border-none bg-primaryclr focus:outline-none focus:border-none focus:ring-0 focus:ring-offset-0 flex items-center space-x-2">
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
            <div className="border-l-2 border-l-secondaryclr h-6 mx-2" />
            <Button
                variant="ghost"
                size="icon"
                className="rounded-full h-12 w-12 mr-1 text-secondaryclr hover:bg-transparent hover:border-none duration-200 focus:outline-none focus:ring-0"
            >
                <Search className="h-5 w-5" />
                <span className="sr-only">Search</span>
            </Button>
        </div>
    );
};

export default SearchBar;

