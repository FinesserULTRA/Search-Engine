import SearchBar from "./SearchBar";

const HomeSearchBar = () => {
    return (
        <div className="w-screen min-h-screen flex items-center bg-primaryclr justify-center">
            <div className="relative w-full max-w-2xl px-4">
                <SearchBar />
            </div>
        </div>
    );
};

export default HomeSearchBar;

